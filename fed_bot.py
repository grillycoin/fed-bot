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


def balance_sheet_signal(new: float, old: float) -> str:
    delta = new - old
    pct   = delta / old * 100
    label = "Fed expanding" if delta > 0 else "Fed shrinking (QT)"
    return f"{label} ({_arrow(delta)}{fmt_trillions(abs(delta))}, {pct:+.2f}%)"


def reserves_signal(new: float, old: float) -> str:
    delta = new - old
    pct   = delta / old * 100
    label = "Reserves rising, more system liquidity" if delta > 0 else "Reserves draining, tighter liquidity"
    return f"{label} ({_arrow(delta)}{fmt_billions(abs(delta))}, {pct:+.2f}%)"


def rrp_signal(new: float, old: float) -> str:
    delta = new - old
    pct   = delta / old * 100 if old else 0
    if delta > 0:
        return f"ON RRP rising, liquidity being drained ({_arrow(delta)}{fmt_billions(abs(delta))}, {pct:+.2f}%)"
    return f"ON RRP falling, liquidity returning to system ({_arrow(delta)}{fmt_billions(abs(delta))}, {pct:+.2f}%)"


def rate_signal(new: float, old: float) -> str:
    delta = new - old
    if delta < 0:
        return f"Fed cut by {abs(delta):.2f}pp → {new:.2f}%"
    if delta > 0:
        return f"Fed hiked by {delta:.2f}pp → {new:.2f}%"
    return "Unchanged"


def yield_curve_signal(new: float, old: float) -> str:
    delta = new - old
    if old < 0 and new >= 0:
        return f"Uninverted — {old:.2f}% → {new:.2f}%"
    if old >= 0 and new < 0:
        return f"Inverted — {old:.2f}% → {new:.2f}%"
    direction = "Steepening" if delta > 0 else "Flattening"
    sign = "+" if delta >= 0 else ""
    return f"{direction} ({sign}{delta:.2f}pp)"


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
    return f"{tone} ({_arrow(delta)}{abs(delta):,.0f}K vs prior)"


def unrate_signal(new: float, old: float) -> str:
    delta = new - old
    if delta > 0.3:
        return f"Rising sharply {old:.1f}% → {new:.1f}%"
    if delta > 0:
        return f"Ticking up {old:.1f}% → {new:.1f}%"
    if delta < -0.3:
        return f"Falling sharply {old:.1f}% → {new:.1f}%"
    return f"Improving {old:.1f}% → {new:.1f}%"


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
        if not result:
            return {}
        # Unwrap nested JSON layers accumulated from old save bug
        data = result
        for _ in range(20):
            try:
                parsed = json.loads(data)
                if isinstance(parsed, dict) and "value" in parsed and "current" not in parsed and "month_snapshot" not in parsed:
                    data = parsed["value"]
                else:
                    return parsed if isinstance(parsed, dict) else {}
            except Exception:
                break
        return {}
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict):
    if UPSTASH_URL:
        # Store raw JSON string — no extra wrapping
        requests.post(
            f"{UPSTASH_URL}/set/{REDIS_KEY}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}", "Content-Type": "text/plain"},
            data=json.dumps(state),
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
    if hist:
        body += f"\n{hist}"
    if sig:
        body += f"\n\n{sig}"
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
# FedWatch probabilities — direct CME methodology
# ---------------------------------------------------------------------------

# All FOMC meeting months in 2026-2027 (used to identify no-FOMC anchor months).
# Must include meetings beyond the 5 we display so anchors are correctly skipped.
_ALL_FOMC_MONTHS: set[tuple[int, int]] = {
    (2026, 1), (2026, 3), (2026, 5), (2026, 6), (2026, 7),
    (2026, 9), (2026, 11), (2026, 12),
    (2027, 1), (2027, 3), (2027, 5), (2027, 6),
}

# ZQ futures CME month codes
_ZQ_CODES = {1:'F',2:'G',3:'H',4:'J',5:'K',6:'M',7:'N',8:'Q',9:'U',10:'V',11:'X',12:'Z'}


def fetch_effr() -> float | None:
    """Latest Effective Federal Funds Rate from FRED (CME uses this, not target midpoint)."""
    obs = fetch_observations("DFF", limit=3)
    return float(obs[0]["value"]) if obs else None


def _zq_rate(year: int, month: int, fallback: float) -> float:
    """Live implied rate for a ZQ futures contract via Yahoo Finance fast_info."""
    try:
        import warnings; warnings.filterwarnings("ignore")
        import yfinance as yf
        sym = f"ZQ{_ZQ_CODES[month]}{str(year)[-2:]}.CBT"
        price = yf.Ticker(sym).fast_info.last_price
        if price:
            return 100.0 - float(price)
    except Exception:
        pass
    return fallback


def fetch_fedwatch() -> str | None:
    """
    Direct CME FedWatch calculation:
      • Uses EFFR (not target midpoint) as the starting rate
      • Uses live ZQ prices via yfinance fast_info
      • Uses no-FOMC month contracts as rate anchors (matches CME anchoring)
      • Cascades binary probabilities for cumulative output
    """
    try:
        import math
        import numpy as np
        from calendar import monthrange
        from dateutil.relativedelta import relativedelta

        state  = load_state()
        lower  = float(state.get("current", {}).get("DFEDTARL", {}).get("value", 3.50))
        upper  = float(state.get("current", {}).get("DFEDTARU", {}).get("value", 3.75))
        effr   = fetch_effr() or (lower + upper) / 2
        print(f"  [FedWatch] EFFR={effr}%  target={lower}-{upper}%")

        # Next 5 upcoming FOMC meetings (after today)
        today = datetime.today()
        all_fomc = [
            datetime(2026, 6, 18), datetime(2026, 7, 29), datetime(2026, 9, 16),
            datetime(2026, 11,  5), datetime(2026, 12, 16),
            datetime(2027,  1, 28), datetime(2027,  3, 17),
        ]
        upcoming = [d for d in all_fomc if d > today][:5]

        def _no_fomc_anchor(after: datetime, before: datetime | None) -> tuple[int, int] | None:
            """First no-FOMC month strictly after `after` and strictly before `before`."""
            m = datetime(after.year, after.month, 1) + relativedelta(months=1)
            end = datetime(before.year, before.month, 1) if before else datetime(2028, 1, 1)
            while m < end:
                if (m.year, m.month) not in _ALL_FOMC_MONTHS:
                    return (m.year, m.month)
                m += relativedelta(months=1)
            return None

        # Build per-meeting binary hike info
        rate_before  = effr
        meeting_data = []

        for i, meeting in enumerate(upcoming):
            yr, mn, day = meeting.year, meeting.month, meeting.day
            days = monthrange(yr, mn)[1]
            n    = day - 1        # days before meeting
            m    = days - day + 1 # days from meeting (inclusive) to month end

            next_meeting = upcoming[i + 1] if i + 1 < len(upcoming) else None
            anchor       = _no_fomc_anchor(meeting, next_meeting)

            if anchor:
                # Use no-FOMC anchor month contract as post-meeting rate
                rate_after = _zq_rate(*anchor, fallback=effr)
            else:
                # Consecutive FOMC months — solve rate_after from this month's contract
                rate_avg   = _zq_rate(yr, mn, fallback=effr)
                rate_after = (rate_avg * days - n * rate_before) / m

            change = (rate_after - rate_before) / 0.25   # in 25 bp units
            h0 = math.trunc(change) * 25
            h1 = h0 + (25 if change >= 0 else -25)
            p1 = abs(change) - math.trunc(abs(change))
            p0 = 1.0 - p1

            meeting_data.append({
                "date":  meeting,
                "sizes": np.array([h0, h1]),
                "probs": np.array([p0, p1]),
            })
            print(f"  [FedWatch] {meeting.strftime('%b %d')}: {rate_before:.4f}% → {rate_after:.4f}%  ({change:+.3f}×25bp)  Hold={p0*100:.1f}%")
            rate_before = rate_after

        # Cascade binary probabilities → cumulative per-meeting distribution
        cum_sizes: np.ndarray | None = None
        cum_probs: np.ndarray | None = None
        lines: list[str] = []

        for info in meeting_data:
            if cum_sizes is None:
                cum_sizes = info["sizes"]
                cum_probs = info["probs"]
            else:
                flat_s = (cum_sizes[:, np.newaxis] + info["sizes"]).flatten()
                flat_p = (cum_probs[:, np.newaxis] * info["probs"]).flatten()
                u, idx = np.unique(flat_s, return_inverse=True)
                cum_sizes = u
                cum_probs = np.bincount(idx, weights=flat_p)

            d = info["date"].strftime("%b %d")
            lines.append(f"<b>FOMC {d}</b>")
            for bp, prob in sorted(zip(cum_sizes.tolist(), cum_probs.tolist()), key=lambda x: -x[1]):
                if prob < 0.005:
                    continue
                bp = round(bp)
                if bp == 0:
                    label = "Hold"
                elif bp > 0:
                    label = f"Hike {bp}bp"
                else:
                    label = f"Cut {abs(bp)}bp"
                lines.append(f"  {label:<12} {prob * 100:.0f}%")
            lines.append("")

        return "\n".join(lines).strip() if lines else None

    except Exception as e:
        print(f"  [FedWatch] Error: {e}")
        import traceback; traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Wednesday rates update
# ---------------------------------------------------------------------------

def build_rates_message(state: dict, now_str: str) -> str:
    current  = state.get("current", {})
    date_str = datetime.now(ET).strftime("%a, %b %d %Y")
    parts    = [f"🎯 <b>Fed Rates Update</b>\n<i>{date_str}</i>\n"]

    # Current rate
    lower = current.get("DFEDTARL", {})
    upper = current.get("DFEDTARU", {})
    if lower and upper:
        d = datetime.strptime(lower["date"], "%Y-%m-%d").strftime("%b %d")
        parts.append(
            f"<b>Fed Funds Target</b>\n"
            f"{fmt_pct(float(lower['value']))} – {fmt_pct(float(upper['value']))}  ·  {d}"
        )

    # FedWatch
    fw = fetch_fedwatch()
    if fw:
        parts.append(f"\n<b>FedWatch — Market Probabilities</b>\n{fw}")

    return "\n".join(parts)


def run_rates():
    """Wednesday rates-only update."""
    print(f"[{datetime.now().isoformat()}] Rates update...")
    state   = load_state()
    refresh_series(state)
    save_state(state)
    now_str = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    msg     = build_rates_message(state, now_str)
    send_telegram(msg)
    print("Rates update sent.")


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
    parser.add_argument("--rates",   action="store_true", help="Wednesday rates update")
    parser.add_argument("--daemon",  action="store_true", help="Run on daily schedule")
    args = parser.parse_args()

    print(f"[{datetime.now().isoformat()}] Starting...")
    try:
        if args.daemon:
            run_daemon()
        elif args.rates:
            run_rates()
        else:
            run(force_weekly=args.weekly, force_monthly=args.monthly)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
