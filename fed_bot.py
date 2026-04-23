#!/usr/bin/env python3
"""
Fed Macro Signal Bot — based on Lancaster's framework

Signals:
  1. Balance sheet / liquidity  (WALCL, WRESBAL, WORAL)     weekly H.4.1
  2. Rate cuts / Fed pivot      (DFEDTARL, DFEDTARU)         daily
  3. Yield curve                (T10Y3M, T10Y2Y)             daily
  4. Payrolls / unemployment    (PAYEMS, UNRATE)              monthly

Schedule (via Render cron, runs daily):
  - Every Friday   → weekly digest of all changed series
  - Last day/month → monthly overview comparing to start-of-month snapshot

State persistence:
  - Local dev : state.json
  - Production: Upstash Redis (set UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN)

Usage:
  python fed_bot.py                  # auto (Friday digest or monthly overview)
  python fed_bot.py --weekly         # force weekly digest
  python fed_bot.py --monthly        # force monthly overview
  python fed_bot.py --daemon         # run daily at 17:30 ET
"""

import argparse
import calendar
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY     = os.environ["FRED_API_KEY"]
TELEGRAM_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

_upstash_url   = os.getenv("UPSTASH_REDIS_REST_URL", "")
_upstash_token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
UPSTASH_URL    = _upstash_url   if _upstash_url.startswith("https://") and "your-db" not in _upstash_url else None
UPSTASH_TOKEN  = _upstash_token if UPSTASH_URL else None
REDIS_KEY     = "fed_bot_state"

STATE_FILE = Path(__file__).parent / "state.json"
ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Series config
# ---------------------------------------------------------------------------

@dataclass
class Series:
    id: str
    label: str
    category: str
    fmt: Callable
    signal: Callable       # (new_val, old_val) -> str
    higher_is_bullish: bool = True  # for monthly overview direction emoji


def fmt_trillions(v: float) -> str:
    return f"${v / 1_000_000:.3f}T"

def fmt_billions(v: float) -> str:
    t = v / 1_000_000
    return f"${t:.3f}T" if t >= 0.01 else f"${v / 1_000:.1f}B"

def fmt_pct(v: float) -> str:
    return f"{v:.2f}%"

def fmt_payrolls(v: float) -> str:
    return f"{v:+,.0f}K" if v != 0 else "0K"

def fmt_unrate(v: float) -> str:
    return f"{v:.1f}%"

def _arrow(delta: float) -> str:
    return "▲" if delta >= 0 else "▼"


def _trend(positive: bool) -> str:
    return "📈" if positive else "📉"


def balance_sheet_signal(new: float, old: float) -> str:
    delta = new - old
    pct   = delta / old * 100
    label = "Fed expanding" if delta > 0 else "Fed shrinking (QT)"
    return f"{_trend(delta > 0)} {label} ({_arrow(delta)}{fmt_trillions(abs(delta))}, {pct:+.2f}%)"


def reserves_signal(new: float, old: float) -> str:
    delta = new - old
    pct   = delta / old * 100
    label = "Reserves rising, more system liquidity" if delta > 0 else "Reserves draining, tighter liquidity"
    return f"{_trend(delta > 0)} {label} ({_arrow(delta)}{fmt_billions(abs(delta))}, {pct:+.2f}%)"


def rrp_signal(new: float, old: float) -> str:
    delta = new - old
    pct   = delta / old * 100 if old else 0
    if delta > 0:
        return f"{_trend(False)} ON RRP rising, liquidity being drained ({_arrow(delta)}{fmt_billions(abs(delta))}, {pct:+.2f}%)"
    return f"{_trend(True)} ON RRP falling, liquidity returning to system ({_arrow(delta)}{fmt_billions(abs(delta))}, {pct:+.2f}%)"


def rate_signal(new: float, old: float) -> str:
    delta = new - old
    if delta < 0:
        return f"{_trend(True)} Fed cut by {abs(delta):.2f}pp → {new:.2f}%"
    if delta > 0:
        return f"{_trend(False)} Fed hiked by {delta:.2f}pp → {new:.2f}%"
    return "➡️ Unchanged"


def yield_curve_signal(new: float, old: float) -> str:
    delta = new - old
    if old < 0 and new >= 0:
        return f"{_trend(True)} Uninverted — {old:.2f}% → {new:.2f}%"
    if old >= 0 and new < 0:
        return f"{_trend(False)} Inverted — {old:.2f}% → {new:.2f}%"
    direction = "Steepening" if delta > 0 else "Flattening"
    sign = "+" if delta >= 0 else ""
    return f"{_trend(delta > 0)} {direction} ({sign}{delta:.2f}pp)"


def payrolls_signal(new: float, old: float) -> str:
    delta = new - old
    if new > 200:
        tone = "Strong print"
    elif new > 100:
        tone = "Solid print"
    elif new > 0:
        tone = "Weak print"
    else:
        tone = "Job losses"
    return f"{_trend(new > 100)} {tone} ({_arrow(delta)}{abs(delta):,.0f}K vs prior)"


def unrate_signal(new: float, old: float) -> str:
    delta = new - old
    if delta > 0.3:
        return f"{_trend(False)} Rising sharply {old:.1f}% → {new:.1f}%"
    if delta > 0:
        return f"{_trend(False)} Ticking up {old:.1f}% → {new:.1f}%"
    if delta < -0.3:
        return f"{_trend(True)} Falling sharply {old:.1f}% → {new:.1f}%"
    return f"{_trend(True)} Improving {old:.1f}% → {new:.1f}%"


SERIES_CONFIG: list[Series] = [
    Series("WALCL",    "Total Fed Assets",                    "1. Balance Sheet", fmt_trillions, balance_sheet_signal, higher_is_bullish=True),
    Series("WRESBAL",  "Reserve Balances at Fed",             "1. Balance Sheet", fmt_billions,  reserves_signal,      higher_is_bullish=True),
    Series("WORAL",    "Overnight Reverse Repos (ON RRP)",    "1. Balance Sheet", fmt_billions,  rrp_signal,           higher_is_bullish=False),
    Series("DFEDTARL", "Fed Funds Target — Lower Bound",      "2. Rates",         fmt_pct,       rate_signal,          higher_is_bullish=False),
    Series("DFEDTARU", "Fed Funds Target — Upper Bound",      "2. Rates",         fmt_pct,       rate_signal,          higher_is_bullish=False),
    Series("T10Y3M",   "Yield Curve 10Y−3M (Lancaster's key)","3. Yield Curve",   fmt_pct,       yield_curve_signal,   higher_is_bullish=True),
    Series("T10Y2Y",   "Yield Curve 10Y−2Y",                  "3. Yield Curve",   fmt_pct,       yield_curve_signal,   higher_is_bullish=True),
    Series("PAYEMS",   "Non-Farm Payrolls (monthly Δ)",       "4. Jobs",          fmt_payrolls,  payrolls_signal,      higher_is_bullish=True),
    Series("UNRATE",   "Unemployment Rate",                   "4. Jobs",          fmt_unrate,    unrate_signal,        higher_is_bullish=False),
]

SERIES_BY_ID = {s.id: s for s in SERIES_CONFIG}

CATEGORY_EMOJI = {
    "1. Balance Sheet": "🏦",
    "2. Rates":         "🎯",
    "3. Yield Curve":   "📈",
    "4. Jobs":          "👷",
}


# ---------------------------------------------------------------------------
# State — Upstash Redis (prod) or local file (dev)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if UPSTASH_URL:
        r = requests.get(
            f"{UPSTASH_URL}/get/{REDIS_KEY}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            timeout=10,
        )
        result = r.json().get("result")
        return json.loads(result) if result else {}
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict):
    if UPSTASH_URL:
        requests.post(
            f"{UPSTASH_URL}/set/{REDIS_KEY}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json={"value": json.dumps(state)},
            timeout=10,
        )
        return
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# FRED
# ---------------------------------------------------------------------------

def fetch_observations(series_id: str, limit: int = 2) -> list[dict]:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return [o for o in r.json().get("observations", []) if o["value"] != "."]
    except Exception as e:
        print(f"  [{series_id}] FRED error: {e}")
        return []


def fetch_latest(series_id: str) -> dict | None:
    obs = fetch_observations(series_id, limit=2)
    return obs[0] if obs else None


def fetch_monthly_snapshots(series_id: str, n_months: int = 3) -> list[dict]:
    """Return one observation per month (most recent in that month) for the last n months."""
    obs = fetch_observations(series_id, limit=n_months * 10)
    if not obs:
        return []
    by_month: dict[str, dict] = {}
    for o in obs:
        key = o["date"][:7]  # YYYY-MM
        if key not in by_month:
            by_month[key] = o  # already sorted desc, so first seen = most recent
    return list(by_month.values())[:n_months]


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
        timeout=15,
    )
    r.raise_for_status()


# ---------------------------------------------------------------------------
# Refresh all series values, return what changed
# ---------------------------------------------------------------------------

def refresh_series(state: dict) -> list[tuple[Series, dict, dict]]:
    """Fetch latest for all series. Returns list of (series, new_obs, last_state_entry)."""
    changed = []
    for series in SERIES_CONFIG:
        obs = fetch_latest(series.id)
        if not obs:
            print(f"  [{series.id}] No data.")
            continue
        last = state.get("current", {}).get(series.id, {})
        if obs["date"] != last.get("date"):
            state.setdefault("current", {})[series.id] = {
                "date": obs["date"],
                "value": obs["value"],
            }
            changed.append((series, obs, last))
            print(f"  [{series.id}] {obs['date']}: {series.fmt(float(obs['value']))}")
        else:
            print(f"  [{series.id}] No new data (last: {obs['date']}).")
    return changed


# ---------------------------------------------------------------------------
# Weekly digest (every Friday)
# ---------------------------------------------------------------------------

DIVIDER = ""

CATEGORY_LABEL = {
    "1. Balance Sheet": "BALANCE SHEET",
    "2. Rates":         "RATES",
    "3. Yield Curve":   "YIELD CURVE",
    "4. Jobs":          "JOBS",
}


def _history_lines(series_id: str, current_date: str) -> str:
    """Return 2 previous monthly snapshots as indented lines, skipping current month."""
    snaps = fetch_monthly_snapshots(series_id, n_months=4)
    current_month = current_date[:7]
    prev = [s for s in snaps if s["date"][:7] != current_month][:2]
    if not prev:
        return ""
    series = SERIES_BY_ID[series_id]
    lines = []
    for s in prev:
        d = datetime.strptime(s["date"], "%Y-%m-%d").strftime("%b %d")
        lines.append(f"<i>{series.fmt(float(s['value']))}  ·  {d}</i>")
    return "\n".join(lines)


def _format_entry(series: "Series", obs: dict, last: dict) -> str:
    new_val  = float(obs["value"])
    date_str = datetime.strptime(obs["date"], "%Y-%m-%d").strftime("%b %d")

    # Special case: merge both rate bounds into one line
    if series.id == "DFEDTARU":
        return None  # handled alongside DFEDTARL

    if series.id == "DFEDTARL":
        value_str = series.fmt(new_val)
        upper_snap = fetch_latest("DFEDTARU")
        upper_str  = f" – {fmt_pct(float(upper_snap['value']))}" if upper_snap else ""
        sig = series.signal(new_val, float(last["value"])) if last.get("value") else "Unchanged"
        hist = _history_lines("DFEDTARL", obs["date"])
        body = f"<b>Fed Funds Target</b>\n{value_str}{upper_str}  ·  {date_str}\n{sig}"
        return body + (f"\n{hist}" if hist else "")

    if last.get("value"):
        sig = series.signal(new_val, float(last["value"]))
    else:
        sig = None

    hist = _history_lines(series.id, obs["date"])
    body = f"<b>{series.label}</b>\n{series.fmt(new_val)}  ·  {date_str}"
    if sig:
        body += f"\n{sig}"
    if hist:
        body += f"\n{hist}"
    return body


def build_weekly_message(changed: list, now_et: str) -> str:
    if not changed:
        return f"📡 <b>Weekly Macro Digest</b>\n<i>{now_et}</i>\n\nNo new data this week."

    blocks: dict[str, list[str]] = {}
    for series, obs, last in changed:
        entry = _format_entry(series, obs, last)
        if entry is not None:
            blocks.setdefault(series.category, []).append(entry)

    date_str = datetime.now(ET).strftime("%a, %b %d %Y")
    parts = [f"📡 <b>Weekly Macro Digest</b>\n<i>{date_str}</i>"]

    for cat in sorted(blocks):
        emoji = CATEGORY_EMOJI.get(cat, "•")
        label = CATEGORY_LABEL.get(cat, cat)
        parts.append(DIVIDER)
        parts.append(f"{emoji} <b>{label}</b>")
        for entry in blocks[cat]:
            parts.append("")
            parts.append(entry)

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Monthly overview (last day of month)
# ---------------------------------------------------------------------------

def build_monthly_message(state: dict, now_et: str, month_label: str) -> str:
    current   = state.get("current", {})
    month_snap = state.get("month_snapshot", {})

    parts = [f"📊 <b>Monthly Overview — {month_label}</b>  <i>{now_et}</i>\n"]

    for cat in ["1. Balance Sheet", "2. Rates", "3. Yield Curve", "4. Jobs"]:
        emoji   = CATEGORY_EMOJI.get(cat, "•")
        section = [f"{emoji} <b>{cat}</b>"]
        any_data = False

        for series in [s for s in SERIES_CONFIG if s.category == cat]:
            cur  = current.get(series.id)
            snap = month_snap.get(series.id)
            if not cur:
                continue
            any_data = True
            cur_val = float(cur["value"])
            if snap and snap.get("value"):
                snap_val = float(snap["value"])
                delta    = cur_val - snap_val
                pct      = delta / abs(snap_val) * 100 if snap_val else 0
                bullish  = (delta > 0) == series.higher_is_bullish
                mood     = "🟢" if bullish else "🔴"
                section.append(
                    f"  {mood} <b>{series.label}</b>\n"
                    f"     Start: {series.fmt(snap_val)}  ({snap['date']})\n"
                    f"     End:   {series.fmt(cur_val)}  ({cur['date']})\n"
                    f"     Δ {_arrow(delta)} {abs(pct):.2f}%"
                )
            else:
                section.append(f"  <b>{series.label}</b>: {series.fmt(cur_val)}  ({cur['date']})")

        if any_data:
            parts.extend(section)
            parts.append("")

    return "\n".join(parts).strip()


def is_last_day_of_month(d: date) -> bool:
    return d.day == calendar.monthrange(d.year, d.month)[1]


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(force_weekly: bool = False, force_monthly: bool = False):
    now_et     = datetime.now(ET)
    today      = now_et.date()
    now_str    = now_et.strftime("%Y-%m-%d %H:%M ET")
    is_friday  = today.weekday() == 4
    is_eom     = is_last_day_of_month(today)

    state    = load_state()
    changed  = refresh_series(state)

    # Seed month snapshot at start of month (day 1) if missing
    if today.day == 1 or "month_snapshot" not in state:
        state["month_snapshot"] = {k: v for k, v in state.get("current", {}).items()}
        print("  [state] Month snapshot seeded.")

    save_state(state)

    if force_monthly or (is_eom and not force_weekly):
        month_label = today.strftime("%B %Y")
        msg = build_monthly_message(state, now_str, month_label)
        send_telegram(msg)
        print(f"Monthly overview sent for {month_label}.")
        # Reset month snapshot for next month
        state["month_snapshot"] = {k: v for k, v in state.get("current", {}).items()}
        save_state(state)

    if force_weekly or is_friday:
        msg = build_weekly_message(changed, now_str)
        send_telegram(msg)
        print(f"Weekly digest sent ({len(changed)} updates).")


def run_daemon():
    try:
        import schedule as sched
    except ImportError:
        print("pip install schedule")
        sys.exit(1)

    print("Daemon — running daily at 17:30 ET.")
    run()
    sched.every().day.at("17:30").do(run)

    import time
    while True:
        sched.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fed macro signal bot")
    parser.add_argument("--weekly",  action="store_true", help="Force weekly digest")
    parser.add_argument("--monthly", action="store_true", help="Force monthly overview")
    parser.add_argument("--daemon",  action="store_true", help="Run on daily schedule")
    args = parser.parse_args()

    print(f"[{datetime.now().isoformat()}] Starting...")
    try:
        if args.daemon:
            run_daemon()
        else:
            run(force_weekly=args.weekly, force_monthly=args.monthly)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
