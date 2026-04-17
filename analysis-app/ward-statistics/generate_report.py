#!/usr/bin/env python3
"""Generate ward statistics dashboard from TTW app-export CSV.

Usage:
    python3 analysis-app/ward-statistics/generate_report.py
    python3 analysis-app/ward-statistics/generate_report.py --input /path/to/Brent.csv
    python3 analysis-app/ward-statistics/generate_report.py --password mysecret
"""

import argparse
import csv
import glob
import json
import os
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("ERROR: This script requires pandas and plotly.\n"
          "Install with: pip install pandas plotly", file=sys.stderr)
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent
WARD_MAP_PATH = SCRIPT_DIR / "ward_polling_districts.csv"
SNAPSHOTS_DIR = SCRIPT_DIR / "snapshots"
EXPORTS_DIR = SCRIPT_DIR / "exports"
OUTPUT_PATH = SCRIPT_DIR / "report.html"
PLOTLY_CACHE_DIR = SCRIPT_DIR / ".plotly_cache"
PLOTLY_CACHE_FILE = PLOTLY_CACHE_DIR / "plotly.min.js"
PLOTLY_CDN_URL = "https://cdn.plot.ly/plotly-2.35.2.min.js"

def _get_plotly_js():
    """Get Plotly JS source (download and cache on first use)."""
    if PLOTLY_CACHE_FILE.exists():
        return PLOTLY_CACHE_FILE.read_text()
    print(f"  Downloading Plotly JS (one-time)...")
    PLOTLY_CACHE_DIR.mkdir(exist_ok=True)
    urllib.request.urlretrieve(PLOTLY_CDN_URL, str(PLOTLY_CACHE_FILE))
    return PLOTLY_CACHE_FILE.read_text()


# TTW sentinel values treated as empty
TTW_EMPTY = {"<NO RECORD>", "<NO DATA RECORDED>", ""}

# Attempt column sets (Most Recent + Previous 1-4)
ATTEMPT_ANSWERED_COLS = [
    "Most Recent Attempt - Answered",
    "Previous 1 - Answered",
    "Previous 2 - Answered",
    "Previous 3 - Answered",
    "Previous 4 - Answered",
]
ATTEMPT_DATE_COLS = [
    "Most Recent Attempt - Date",
    "Previous 1 - Date",
    "Previous 2 - Date",
    "Previous 3 - Date",
    "Previous 4 - Date",
]

# LE2026 GVI columns (Most Recent + Previous 1-4)
LE2026_PREFIX = "Brent London Borough Council election (2026-May-07)"
LE2026_GVI_COLS = [
    f"{LE2026_PREFIX} Most Recent Data - GVI",
    f"{LE2026_PREFIX} Previous Data 1 - GVI",
    f"{LE2026_PREFIX} Previous Data 2 - GVI",
    f"{LE2026_PREFIX} Previous Data 3 - GVI",
    f"{LE2026_PREFIX} Previous Data 4 - GVI",
]
LE2026_PARTY_COLS = [
    f"{LE2026_PREFIX} Most Recent Data - Usual Party",
    f"{LE2026_PREFIX} Previous Data 1 - Usual Party",
    f"{LE2026_PREFIX} Previous Data 2 - Usual Party",
    f"{LE2026_PREFIX} Previous Data 3 - Usual Party",
    f"{LE2026_PREFIX} Previous Data 4 - Usual Party",
]
LE2026_POSTAL_COL = f"{LE2026_PREFIX} Most Recent Data - Postal Voter"

GE2024_PREFIX = "Brent London Borough Council election (2024-Jul-04)"
GE2024_VOTED_COL = f"{GE2024_PREFIX} Voted"

# All known party values
PARTY_ORDER = ["Greens", "Labour", "Conservatives", "Liberal Democrats",
               "Reform/UKIP/Brexit", "Plaid Cymru", "Independent",
               "Residents Association", "Others"]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def find_input_csv():
    """Find the most recent Brent*.csv in ~/Downloads."""
    downloads = Path.home() / "Downloads"
    candidates = list(downloads.glob("Brent*.csv"))
    if not candidates:
        print("ERROR: No Brent*.csv found in ~/Downloads. Use --input.", file=sys.stderr)
        sys.exit(1)
    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    return str(newest)


def load_ward_map():
    """Load ward_polling_districts.csv → dict[polling_district] → ward."""
    mapping = {}
    with open(WARD_MAP_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row["polling_district"]] = row["ward"]
    return mapping


def _is_empty(val):
    """Check if a value is a TTW empty sentinel."""
    return (val or "").strip() in TTW_EMPTY


def _first_nonempty(row, cols):
    """Return first non-empty value from a list of columns (fallback chain)."""
    for col in cols:
        val = row.get(col, "")
        if not _is_empty(val):
            return val.strip()
    return ""


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------

def compute_ward_stats(df, ward_map):
    """Compute all statistics per ward. Returns dict[ward] → stats dict."""

    # Extract polling district from Voter Number
    df = df.copy()
    df["_pd"] = df["Voter Number"].astype(str).str.split("-").str[0]
    df["_ward"] = df["_pd"].map(ward_map)

    # Filter out unmapped rows
    unmapped = df["_ward"].isna()
    if unmapped.any():
        n = unmapped.sum()
        unrecognised = df[unmapped]["_pd"].value_counts()
        print(f"  Note: {n} rows with unmapped polling districts (filtered out):")
        for pd_code, count in unrecognised.items():
            print(f"    {pd_code}: {count} rows")
    df = df[~unmapped].copy()

    # Build door key
    address_cols = ["Post Code", "House Name", "House Number", "Road"]
    missing_addr = [col for col in address_cols if col not in df.columns]
    if missing_addr:
        print(f"  WARNING: Address columns missing from CSV: {missing_addr}")
        print(f"  Door counts will be unreliable — ensure the full unredacted export is used.")
    for col in address_cols:
        if col not in df.columns:
            df[col] = ""
    df["_door"] = (df["Post Code"].fillna("").str.strip() + "|" +
                   df["House Name"].fillna("").str.strip() + "|" +
                   df["House Number"].fillna("").astype(str).str.strip() + "|" +
                   df["Road"].fillna("").str.strip())

    # Pre-compute per-voter flags
    def _has_attempt(row):
        return any(row.get(c, "") in ("Y", "N") for c in ATTEMPT_ANSWERED_COLS)

    def _has_answered(row):
        return any(row.get(c, "") == "Y" for c in ATTEMPT_ANSWERED_COLS)

    def _get_gvi(row):
        return _first_nonempty(row, LE2026_GVI_COLS)

    def _get_party(row):
        return _first_nonempty(row, LE2026_PARTY_COLS)

    def _is_dnk_or_nla(row):
        dnk = str(row.get("Do Not Knock ticked", "")).strip().lower()
        nla = str(row.get("No Longer at Address ticked", "")).strip().lower()
        return dnk in ("true", "1", "y") or nla in ("true", "1", "y")

    # Convert to records for row-level processing
    records = df.to_dict("records")

    # Per-voter computations
    for r in records:
        gvi = _get_gvi(r)
        party = _get_party(r)
        has_canvass_data = bool(gvi) or bool(party)
        r["_attempted"] = _has_attempt(r) or has_canvass_data
        r["_answered"] = _has_answered(r) or has_canvass_data
        r["_gvi"] = gvi
        r["_party"] = party
        r["_dnk_nla"] = _is_dnk_or_nla(r)
        r["_ge2024_voted"] = str(r.get(GE2024_VOTED_COL, "")).strip() == "Y"
        postal_val = str(r.get(LE2026_POSTAL_COL, "")).strip()
        r["_postal"] = postal_val == "Y"

    # Sanity check: flag any non-empty Usual Party values not in PARTY_ORDER.
    # TTW party labelling is supposed to be rigid, so anything unexpected here
    # either means a new party code needs to be added to PARTY_ORDER or the
    # source data has drifted.
    unknown_parties = Counter()
    for r in records:
        party = r["_party"]
        if party and party not in TTW_EMPTY and party not in PARTY_ORDER:
            unknown_parties[party] += 1
    if unknown_parties:
        print(f"  WARNING: {sum(unknown_parties.values())} voters have a Usual Party "
              f"value not in PARTY_ORDER — these will be excluded from party charts:")
        for party, count in unknown_parties.most_common():
            print(f"    {party!r}: {count} voters")
        print(f"  If this is a legitimate party value, add it to PARTY_ORDER "
              f"(and party_colors in generate_html).")

    # Group by ward
    ward_voters = defaultdict(list)
    for r in records:
        ward_voters[r["_ward"]].append(r)

    stats = {}
    for ward, voters in sorted(ward_voters.items()):
        # Door-level aggregation
        doors = defaultdict(list)
        for v in voters:
            doors[v["_door"]].append(v)

        total_doors = len(doors)
        total_voters = len(voters)

        # Knockable doors: exclude doors where ALL voters are DNK or NLA
        knockable_doors = sum(1 for d_voters in doors.values()
                              if not all(v["_dnk_nla"] for v in d_voters))

        # Doors knocked: at least one voter attempted
        doors_knocked = sum(1 for d_voters in doors.values()
                            if any(v["_attempted"] for v in d_voters))

        # Doors answered: at least one voter answered
        doors_answered = sum(1 for d_voters in doors.values()
                             if any(v["_answered"] for v in d_voters))

        # DNK/NLA-only door count (all voters at address are DNK or NLA)
        # = total_doors - knockable_doors
        dnk_nla_doors = total_doors - knockable_doors

        # GVI breakdown (use fallback chain per voter)
        gvi_counts = Counter()
        gvi_denominator = 0
        for v in voters:
            gvi = v["_gvi"]
            if gvi in ("1", "2", "3", "4", "5"):
                gvi_counts[gvi] += 1
                gvi_denominator += 1

        # Answered but no GVI
        voters_answered = sum(1 for v in voters if v["_answered"])
        gvi_answered_no_record = voters_answered - gvi_denominator
        if gvi_answered_no_record < 0:
            gvi_answered_no_record = 0

        # Party breakdown
        party_counts = Counter()
        party_denominator = 0
        for v in voters:
            party = v["_party"]
            if party and party not in TTW_EMPTY:
                party_counts[party] += 1
                party_denominator += 1

        # Party x GVI cross-tab: party_gvi[party][gvi] = count
        party_gvi = {p: {str(i): 0 for i in range(1, 6)} for p in PARTY_ORDER}
        for v in voters:
            party = v["_party"]
            gvi = v["_gvi"]
            if party in PARTY_ORDER and gvi in ("1", "2", "3", "4", "5"):
                party_gvi[party][gvi] += 1

        # GE2024 voted
        ge2024_voted = sum(1 for v in voters if v["_ge2024_voted"])
        ge2024_voted_contacted = sum(1 for v in voters
                                      if v["_ge2024_voted"] and v["_answered"])

        # Postal voter
        postal_voters = sum(1 for v in voters if v["_postal"])
        postal_not_contacted = sum(1 for v in voters
                                    if v["_postal"] and not v["_answered"])

        stats[ward] = {
            "total_doors": total_doors,
            "total_voters": total_voters,
            "knockable_doors": knockable_doors,
            "dnk_nla_doors": dnk_nla_doors,
            "doors_knocked": doors_knocked,
            "doors_answered": doors_answered,
            "voters_answered": voters_answered,
            "gvi_denominator": gvi_denominator,
            "gvi": {str(i): gvi_counts.get(str(i), 0) for i in range(1, 6)},
            "gvi_answered_no_record": gvi_answered_no_record,
            "party_denominator": party_denominator,
            "party": {p: party_counts.get(p, 0) for p in PARTY_ORDER},
            "party_gvi": party_gvi,
            "ge2024_voted": ge2024_voted,
            "ge2024_voted_contacted": ge2024_voted_contacted,
            "postal_voters": postal_voters,
            "postal_not_contacted": postal_not_contacted,
        }

    return stats


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def save_snapshot(stats, source_file, source_rows):
    """Save stats to a timestamped JSON snapshot. Skips if stats unchanged from last snapshot."""
    SNAPSHOTS_DIR.mkdir(exist_ok=True)

    # Check if stats have changed since last snapshot
    existing = sorted(SNAPSHOTS_DIR.glob("stats_*.json"))
    if existing:
        try:
            last = json.loads(existing[-1].read_text())
            if last.get("wards") == stats:
                return None  # No change
        except (json.JSONDecodeError, OSError):
            pass

    now = datetime.now()
    snapshot = {
        "timestamp": now.isoformat(timespec="seconds"),
        "date": now.strftime("%Y-%m-%d"),
        "source_file": Path(source_file).name,
        "source_rows": source_rows,
        "wards": stats,
    }
    filename = f"stats_{now.strftime('%Y-%m-%d_%H%M%S')}.json"
    path = SNAPSHOTS_DIR / filename
    path.write_text(json.dumps(snapshot, indent=2))
    return str(path)


def cleanup_snapshots(max_age_days=365):
    """Delete snapshots older than max_age_days."""
    if not SNAPSHOTS_DIR.exists():
        return
    cutoff = datetime.now() - timedelta(days=max_age_days)
    removed = 0
    for f in SNAPSHOTS_DIR.glob("stats_*.json"):
        try:
            # Parse date from filename
            date_str = f.stem.replace("stats_", "")[:10]
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff:
                f.unlink()
                removed += 1
        except (ValueError, OSError):
            pass
    return removed


def load_snapshots():
    """Load all snapshots, sorted by date. Returns list of snapshot dicts."""
    if not SNAPSHOTS_DIR.exists():
        return []
    snapshots = []
    for f in sorted(SNAPSHOTS_DIR.glob("stats_*.json")):
        try:
            data = json.loads(f.read_text())
            snapshots.append(data)
        except (json.JSONDecodeError, OSError):
            pass
    return snapshots


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def _party_slug(name):
    """Lower-snake-case party name for CSV column headers."""
    return (name.lower()
                .replace(" ", "_")
                .replace("/", "_"))


def export_csvs(stats, wards):
    """Write ward_stats_<date>.csv and ward_party_gvi_<date>.csv into exports/.

    ward_stats.csv: one row per ward plus an "All Brent" totals row.
    ward_party_gvi.csv: one row per (ward, party) in wide format (GVI 1-5 counts
    and percentages). Rows where a party has no data in a ward are dropped.
    An "All Brent" block of rows is appended for convenience.
    """
    EXPORTS_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Cross-ward totals (synthesised; not part of per-ward stats dict)
    total = {
        "total_doors": sum(stats[w]["total_doors"] for w in wards),
        "dnk_nla_doors": sum(stats[w]["dnk_nla_doors"] for w in wards),
        "doors_knocked": sum(stats[w]["doors_knocked"] for w in wards),
        "doors_answered": sum(stats[w]["doors_answered"] for w in wards),
        "voters_answered": sum(stats[w]["voters_answered"] for w in wards),
        "gvi_denominator": sum(stats[w]["gvi_denominator"] for w in wards),
        "gvi": {str(i): sum(stats[w]["gvi"][str(i)] for w in wards)
                for i in range(1, 6)},
        "party_denominator": sum(stats[w]["party_denominator"] for w in wards),
        "party": {p: sum(stats[w]["party"].get(p, 0) for w in wards)
                  for p in PARTY_ORDER},
        "party_gvi": {
            p: {str(i): sum(stats[w].get("party_gvi", {})
                                       .get(p, {})
                                       .get(str(i), 0) for w in wards)
                for i in range(1, 6)}
            for p in PARTY_ORDER
        },
        "ge2024_voted": sum(stats[w]["ge2024_voted"] for w in wards),
        "ge2024_voted_contacted": sum(stats[w]["ge2024_voted_contacted"]
                                      for w in wards),
        "postal_voters": sum(stats[w]["postal_voters"] for w in wards),
        "postal_not_contacted": sum(stats[w]["postal_not_contacted"]
                                    for w in wards),
    }

    # --- ward_stats.csv ---
    stats_path = EXPORTS_DIR / f"ward_stats_{date_str}.csv"
    fieldnames = [
        "ward",
        "total_doors", "dnk_nla_doors", "doors_knocked", "doors_answered",
        "knocked_pct", "answered_pct", "gvi_capture_pct",
        "voters_answered", "gvi_denominator",
        "gvi_1", "gvi_2", "gvi_3", "gvi_4", "gvi_5",
        "party_denominator",
    ] + [f"party_{_party_slug(p)}" for p in PARTY_ORDER] + [
        "ge2024_voted", "ge2024_voted_contacted",
        "postal_voters", "postal_not_contacted",
    ]

    def _row(ward_name, s):
        td = s["total_doors"]
        dk = s["doors_knocked"]
        da = s["doors_answered"]
        va = s["voters_answered"]
        gd = s["gvi_denominator"]
        row = {
            "ward": ward_name,
            "total_doors": td,
            "dnk_nla_doors": s["dnk_nla_doors"],
            "doors_knocked": dk,
            "doors_answered": da,
            "knocked_pct": f"{(dk / td * 100) if td else 0:.1f}",
            "answered_pct": f"{(da / td * 100) if td else 0:.1f}",
            "gvi_capture_pct": f"{(gd / va * 100) if va else 0:.1f}",
            "voters_answered": va,
            "gvi_denominator": gd,
            "party_denominator": s["party_denominator"],
            "ge2024_voted": s["ge2024_voted"],
            "ge2024_voted_contacted": s["ge2024_voted_contacted"],
            "postal_voters": s["postal_voters"],
            "postal_not_contacted": s["postal_not_contacted"],
        }
        for i in range(1, 6):
            row[f"gvi_{i}"] = s["gvi"][str(i)]
        for p in PARTY_ORDER:
            row[f"party_{_party_slug(p)}"] = s["party"].get(p, 0)
        return row

    with open(stats_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ward in wards:
            writer.writerow(_row(ward, stats[ward]))
        writer.writerow(_row("All Brent", total))

    # --- ward_party_gvi.csv ---
    pg_path = EXPORTS_DIR / f"ward_party_gvi_{date_str}.csv"
    pg_fieldnames = [
        "ward", "party", "n",
        "gvi_1", "gvi_2", "gvi_3", "gvi_4", "gvi_5",
        "gvi_1_pct", "gvi_2_pct", "gvi_3_pct", "gvi_4_pct", "gvi_5_pct",
    ]

    def _pg_rows(label, pg):
        out = []
        for party in PARTY_ORDER:
            counts = pg.get(party, {})
            n = sum(counts.get(str(i), 0) for i in range(1, 6))
            if n == 0:
                continue
            row = {"ward": label, "party": party, "n": n}
            for i in range(1, 6):
                c = counts.get(str(i), 0)
                row[f"gvi_{i}"] = c
                row[f"gvi_{i}_pct"] = f"{(c / n * 100):.1f}"
            out.append(row)
        return out

    with open(pg_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=pg_fieldnames)
        writer.writeheader()
        for ward in wards:
            for row in _pg_rows(ward, stats[ward].get("party_gvi", {})):
                writer.writerow(row)
        for row in _pg_rows("All Brent", total["party_gvi"]):
            writer.writerow(row)

    return stats_path, pg_path


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def generate_html(stats, snapshots, password=None, plotly_js=""):
    """Generate interactive HTML dashboard."""
    wards = sorted(stats.keys())
    colors = {
        "Brondesbury Park": "#1f77b4",
        "Harlesden and Kensal Green": "#ff7f0e",
        "Kilburn": "#2ca02c",
        "Northwick Park": "#d62728",
        "Roundwood": "#9467bd",
        "Wembley Park": "#8c564b",
        "Willesden Green": "#e377c2",
    }

    # --- Pre-compute per-ward metrics ---
    # Use total_doors as denominator (matches TTW, includes DNK/NLA)
    metrics = []
    for ward in wards:
        s = stats[ward]
        total = s["total_doors"]
        knocked_pct = (s["doors_knocked"] / total * 100) if total else 0
        answered_pct = (s["doors_answered"] / total * 100) if total else 0
        answer_rate = (s["doors_answered"] / s["doors_knocked"] * 100) if s["doors_knocked"] else 0
        gvi1_pct = (s["gvi"]["1"] / s["gvi_denominator"] * 100) if s["gvi_denominator"] else 0
        not_knocked = total - s["doors_knocked"]
        knocked_no_answer = s["doors_knocked"] - s["doors_answered"]
        metrics.append({
            "ward": ward,
            "knocked_pct": knocked_pct,
            "answered_pct": answered_pct,
            "answer_rate": answer_rate,
            "gvi1_pct": gvi1_pct,
            "not_knocked": not_knocked,
            "knocked_no_answer": knocked_no_answer,
            "answered": s["doors_answered"],
            "total_doors": total,
        })

    # --- Low-answered-ward warnings ---
    # For charts with wards on the x-axis: the warning is rendered inline under
    # each affected ward's tick label (see ward_tick_labels below).
    # For views without wards on the x-axis (summary table, by-ward table, trend
    # chart): use a small banner listing affected wards.
    LOW_ANSWER_THRESHOLD = 10.0
    answered_pct_map = {m["ward"]: m["answered_pct"] for m in metrics}
    low_answer = sorted(
        [(m["ward"], m["answered_pct"]) for m in metrics
         if m["answered_pct"] < LOW_ANSWER_THRESHOLD],
        key=lambda x: x[1],
    )
    low_wards_set = {w for w, _ in low_answer}

    # Per-ward tick labels: affected wards get a red "low ans X%" suffix under
    # the ward name. Used as the displayed tick text on ward-axis charts.
    ward_tick_labels = []
    for w in wards:
        pct = answered_pct_map.get(w, 0)
        if w in low_wards_set:
            ward_tick_labels.append(
                f"{w}<br><span style='color:#c0392b;font-size:10px'>"
                f"low ans {pct:.1f}%</span>"
            )
        else:
            ward_tick_labels.append(w)

    if low_answer:
        parts = ", ".join(f"{w} ({p:.1f}%)" for w, p in low_answer)
        low_warn = (
            '<p class="low-answer-warning">'
            f'&#9888; Low answered % (&lt;{LOW_ANSWER_THRESHOLD:.0f}%): {parts}. '
            'Any per-ward conclusions here rest on very small samples and should '
            'be interpreted with caution.'
            '</p>'
        )
    else:
        low_warn = ""

    # --- Cross-ward totals ---
    # Appear as a highlighted "All Brent" bar on ward-by-ward charts that
    # benefit from a reference total (GVI breakdown, Party breakdown, Cross-ward
    # rates). Excluded from the canvassing-status chart, GE2024 chart, and
    # time-series trends per product requirements.
    TOTAL_LABEL = "All Brent"
    total_doors_all = sum(stats[w]["total_doors"] for w in wards)
    total_knocked_all = sum(stats[w]["doors_knocked"] for w in wards)
    total_answered_all = sum(stats[w]["doors_answered"] for w in wards)
    total_gvi = {str(i): sum(stats[w]["gvi"][str(i)] for w in wards) for i in range(1, 6)}
    total_gvi_denom = sum(stats[w]["gvi_denominator"] for w in wards)
    total_party = {p: sum(stats[w]["party"].get(p, 0) for w in wards) for p in PARTY_ORDER}
    total_party_denom = sum(stats[w]["party_denominator"] for w in wards)

    total_knocked_pct = (total_knocked_all / total_doors_all * 100) if total_doors_all else 0
    total_answered_pct = (total_answered_all / total_doors_all * 100) if total_doors_all else 0
    total_answer_rate = (total_answered_all / total_knocked_all * 100) if total_knocked_all else 0

    # x-axis configuration with the totals column appended and highlighted
    wards_with_total = list(wards) + [TOTAL_LABEL]
    total_tick = (
        f"<b style='color:#1f618d'>{TOTAL_LABEL}</b>"
    )
    ward_tick_labels_with_total = ward_tick_labels + [total_tick]
    _ward_xaxis_with_total = dict(
        tickangle=-30, tickmode="array",
        tickvals=wards_with_total, ticktext=ward_tick_labels_with_total,
    )

    # --- Chart: Canvassing status (stacked bar — like GVI/Party) ---
    fig_status = go.Figure()
    status_items = [
        ("Not Yet Knocked", [m["not_knocked"] for m in metrics], "#bdc3c7"),
        ("Knocked - No Answer", [m["knocked_no_answer"] for m in metrics], "#e67e22"),
        ("Answered", [m["answered"] for m in metrics], "#27ae60"),
    ]
    for label, vals, color in status_items:
        hover = []
        pcts = []
        for i, m in enumerate(metrics):
            pct = (vals[i] / m["total_doors"] * 100) if m["total_doors"] else 0
            pcts.append(pct)
            hover.append(f"{m['ward']}<br>{label}: {vals[i]} ({pct:.1f}%)")
        fig_status.add_trace(go.Bar(
            name=label, x=wards, y=pcts,
            marker_color=color,
            hovertext=hover, hoverinfo="text",
        ))
    # xaxis config for charts with wards on the x-axis (inline per-ward warnings)
    _ward_xaxis = dict(tickangle=-30, tickmode="array",
                       tickvals=wards, ticktext=ward_tick_labels)
    # xaxis config for non-ward-axis charts (e.g. aggregate fig_party_gvi)
    _bar_xaxis = dict(tickangle=-30)
    fig_status.update_layout(
        title="Canvassing Status by Ward",
        barmode="stack", yaxis_title="% of total doors",
        height=500, template="plotly_white", xaxis=_ward_xaxis,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="center", x=0.5),
    )

    # --- Chart: Cross-ward comparison (renamed metrics) ---
    fig_comparison = go.Figure()
    coverage_y = [m["knocked_pct"] for m in metrics] + [total_knocked_pct]
    contact_y = [m["answered_pct"] for m in metrics] + [total_answered_pct]
    open_y = [m["answer_rate"] for m in metrics] + [total_answer_rate]
    fig_comparison.add_trace(go.Bar(
        name="Coverage (knocked / total doors)", x=wards_with_total,
        y=coverage_y,
        text=[f"{v:.1f}%" for v in coverage_y],
        textposition="auto",
        hovertemplate="%{x}<br>Coverage: %{y:.1f}% of total doors<extra></extra>",
    ))
    fig_comparison.add_trace(go.Bar(
        name="Contact Rate (answered / total doors)", x=wards_with_total,
        y=contact_y,
        text=[f"{v:.1f}%" for v in contact_y],
        textposition="auto",
        hovertemplate="%{x}<br>Contact Rate: %{y:.1f}% of total doors<extra></extra>",
    ))
    fig_comparison.add_trace(go.Bar(
        name="Door Open Rate (answered / knocked)", x=wards_with_total,
        y=open_y,
        text=[f"{v:.1f}%" for v in open_y],
        textposition="auto",
        hovertemplate="%{x}<br>Door Open Rate: %{y:.1f}% of knocked doors<extra></extra>",
    ))
    fig_comparison.update_layout(
        title="Cross-Ward Canvassing Rates",
        barmode="group", yaxis_title="Percentage",
        height=500, template="plotly_white", xaxis=_ward_xaxis_with_total,
    )

    # --- Chart: GVI breakdown (stacked bar for all wards) ---
    fig_gvi = go.Figure()
    gvi_labels = ["1 (Green)", "2", "3", "4", "5 (Opposition)"]
    gvi_colors = ["#27ae60", "#82e0aa", "#f7dc6f", "#e59866", "#e74c3c"]
    for i, (label, color) in enumerate(zip(gvi_labels, gvi_colors), 1):
        vals = []
        hover = []
        for ward in wards:
            s = stats[ward]
            count = s["gvi"][str(i)]
            pct = (count / s["gvi_denominator"] * 100) if s["gvi_denominator"] else 0
            vals.append(pct)
            hover.append(f"{ward}<br>GVI {i}: {count} ({pct:.1f}%)")
        # Totals bar
        tcount = total_gvi[str(i)]
        tpct = (tcount / total_gvi_denom * 100) if total_gvi_denom else 0
        vals.append(tpct)
        hover.append(f"{TOTAL_LABEL}<br>GVI {i}: {tcount} ({tpct:.1f}%)")
        fig_gvi.add_trace(go.Bar(
            name=label, x=wards_with_total, y=vals,
            marker_color=color,
            hovertext=hover, hoverinfo="text",
        ))
    fig_gvi.update_layout(
        title="Green Voting Intention Breakdown by Ward",
        barmode="stack", yaxis_title="% of canvassed voters",
        height=500, template="plotly_white", xaxis=_ward_xaxis_with_total,
    )

    # --- Chart 4: Party breakdown (stacked bar) ---
    fig_party = go.Figure()
    party_colors = {
        "Greens": "#6AB023", "Labour": "#DC241f", "Conservatives": "#0087DC",
        "Liberal Democrats": "#FDBB30", "Reform/UKIP/Brexit": "#12B6CF",
        "Plaid Cymru": "#005B54", "Independent": "#808080",
        "Residents Association": "#556B2F", "Others": "#999999",
    }
    for party in PARTY_ORDER:
        vals = []
        hover = []
        for ward in wards:
            s = stats[ward]
            count = s["party"].get(party, 0)
            pct = (count / s["party_denominator"] * 100) if s["party_denominator"] else 0
            vals.append(pct)
            hover.append(f"{ward}<br>{party}: {count} ({pct:.1f}%)")
        # Totals bar
        tcount = total_party.get(party, 0)
        tpct = (tcount / total_party_denom * 100) if total_party_denom else 0
        vals.append(tpct)
        hover.append(f"{TOTAL_LABEL}<br>{party}: {tcount} ({tpct:.1f}%)")
        fig_party.add_trace(go.Bar(
            name=party, x=wards_with_total, y=vals,
            marker_color=party_colors.get(party, "#999"),
            hovertext=hover, hoverinfo="text",
        ))
    fig_party.update_layout(
        title="Usual Party Breakdown by Ward",
        barmode="stack", yaxis_title="% of voters with party data",
        height=500, template="plotly_white", xaxis=_ward_xaxis_with_total,
    )

    # --- Chart: GVI breakdown within each Usual Party (aggregate across wards) ---
    # For each party, compute the distribution of GVI values among voters with that party.
    agg_party_gvi = {p: {str(i): 0 for i in range(1, 6)} for p in PARTY_ORDER}
    for ward in wards:
        pg = stats[ward].get("party_gvi", {})
        for party in PARTY_ORDER:
            for i in range(1, 6):
                agg_party_gvi[party][str(i)] += pg.get(party, {}).get(str(i), 0)
    # Denominator per party = sum across GVI 1-5
    party_gvi_denom = {p: sum(agg_party_gvi[p].values()) for p in PARTY_ORDER}

    fig_party_gvi = go.Figure()
    for i, (label, color) in enumerate(zip(gvi_labels, gvi_colors), 1):
        vals = []
        hover = []
        for party in PARTY_ORDER:
            count = agg_party_gvi[party][str(i)]
            denom = party_gvi_denom[party]
            pct = (count / denom * 100) if denom else 0
            vals.append(pct)
            hover.append(f"{party}<br>GVI {i}: {count} of {denom} ({pct:.1f}%)")
        fig_party_gvi.add_trace(go.Bar(
            name=label, x=PARTY_ORDER, y=vals,
            marker_color=color,
            hovertext=hover, hoverinfo="text",
        ))
    fig_party_gvi.update_layout(
        title="GVI Breakdown within each Usual Party (all wards)",
        barmode="stack", yaxis_title="% of voters with that Usual Party",
        height=500, template="plotly_white", xaxis=_bar_xaxis,
    )

    # --- Chart: Per-ward Party x GVI (small multiples / facet grid) ---
    # Same data as fig_party_gvi but disaggregated by ward so each ward's
    # swing-percentage pattern can be compared side-by-side.
    PARTY_ABBREV = {
        "Greens": "Green", "Labour": "Lab", "Conservatives": "Con",
        "Liberal Democrats": "LD", "Reform/UKIP/Brexit": "Ref",
        "Plaid Cymru": "PC", "Independent": "Ind",
        "Residents Association": "RA", "Others": "Other",
    }
    party_x_labels = [PARTY_ABBREV.get(p, p) for p in PARTY_ORDER]

    n_cols_pw = 4
    n_rows_pw = (len(wards) + n_cols_pw - 1) // n_cols_pw
    subplot_titles_pw = []
    for w in wards:
        if w in low_wards_set:
            pct = answered_pct_map[w]
            subplot_titles_pw.append(
                f"{w}<br><span style='color:#c0392b;font-size:10px'>"
                f"low ans {pct:.1f}%</span>"
            )
        else:
            subplot_titles_pw.append(w)

    fig_party_gvi_wards = make_subplots(
        rows=n_rows_pw, cols=n_cols_pw,
        subplot_titles=subplot_titles_pw,
        vertical_spacing=0.2,
        horizontal_spacing=0.05,
    )
    for idx, ward in enumerate(wards):
        r = idx // n_cols_pw + 1
        c = idx % n_cols_pw + 1
        pg = stats[ward].get("party_gvi", {})
        party_denoms = {p: sum(pg.get(p, {}).get(str(j), 0) for j in range(1, 6))
                        for p in PARTY_ORDER}
        for i, (label, color) in enumerate(zip(gvi_labels, gvi_colors), 1):
            vals = []
            hover = []
            for party in PARTY_ORDER:
                count = pg.get(party, {}).get(str(i), 0)
                denom = party_denoms[party]
                pct = (count / denom * 100) if denom else 0
                vals.append(pct)
                hover.append(f"{ward} — {party}<br>GVI {i}: {count} of {denom} ({pct:.1f}%)")
            fig_party_gvi_wards.add_trace(
                go.Bar(
                    name=label, x=party_x_labels, y=vals,
                    marker_color=color,
                    hovertext=hover, hoverinfo="text",
                    legendgroup=label, showlegend=(idx == 0),
                ),
                row=r, col=c,
            )
    fig_party_gvi_wards.update_layout(
        title=dict(text="Usual Party &times; GVI breakdown — per ward",
                   y=0.98, yanchor="top"),
        barmode="stack",
        height=750, template="plotly_white",
        margin=dict(t=90, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.08,
                    xanchor="center", x=0.5),
    )
    fig_party_gvi_wards.update_xaxes(tickangle=-45, tickfont=dict(size=9))
    fig_party_gvi_wards.update_yaxes(range=[0, 100], tickfont=dict(size=9))

    # --- Chart 6: GE2024 voters contacted ---
    fig_ge2024 = go.Figure()
    ge_vals = []
    ge_hover = []
    has_ge_data = False
    for ward in wards:
        s = stats[ward]
        if s["ge2024_voted"] > 0:
            has_ge_data = True
        pct = (s["ge2024_voted_contacted"] / s["ge2024_voted"] * 100) if s["ge2024_voted"] else 0
        ge_vals.append(pct)
        ge_hover.append(f"{ward}<br>GE2024 Voted: {s['ge2024_voted']}<br>"
                        f"Contacted: {s['ge2024_voted_contacted']} ({pct:.1f}%)")
    fig_ge2024.add_trace(go.Bar(
        x=wards, y=ge_vals,
        text=[f"{v:.1f}%" for v in ge_vals],
        textposition="auto",
        hovertext=ge_hover, hoverinfo="text",
        marker_color="#3498db",
    ))
    fig_ge2024.update_layout(
        title="GE2024 Voters Contacted for LE2026" +
              (" (some wards may have no GE2024 data)" if not all(
                  stats[w]["ge2024_voted"] > 0 for w in wards) else ""),
        yaxis_title="% of GE2024 voters spoken to",
        height=500, template="plotly_white", xaxis=_bar_xaxis,
    )

    # --- Chart 7: Trend lines (if snapshots available) ---
    # Deduplicate snapshots per date (keep the latest per day)
    daily_snapshots = {}
    for snap in snapshots:
        daily_snapshots[snap["date"]] = snap  # last one per date wins
    unique_snapshots = [daily_snapshots[d] for d in sorted(daily_snapshots.keys())]

    fig_trend = None
    if len(unique_snapshots) >= 2:
        fig_trend = make_subplots(rows=2, cols=2,
                                   subplot_titles=["Knocked %", "Answered %",
                                                    "GVI 1 (Green) %", "GVI 2 (Lean Green) %"])
        for ward in wards:
            dates = []
            knocked_pcts = []
            answered_pcts = []
            gvi1_pcts = []
            gvi2_pcts = []
            for snap in unique_snapshots:
                if ward in snap.get("wards", {}):
                    s = snap["wards"][ward]
                    dates.append(snap["date"])
                    total = s.get("total_doors", 1)
                    knocked_pcts.append(
                        s["doors_knocked"] / total * 100 if total else 0)
                    answered_pcts.append(
                        s["doors_answered"] / total * 100 if total else 0)
                    gvi_denom = s.get("gvi_denominator", 0)
                    gvi = s.get("gvi", {})
                    gvi1_pcts.append(
                        int(gvi.get("1", 0)) / gvi_denom * 100 if gvi_denom else 0)
                    gvi2_pcts.append(
                        int(gvi.get("2", 0)) / gvi_denom * 100 if gvi_denom else 0)
            if dates:
                fig_trend.add_trace(go.Scatter(
                    x=dates, y=knocked_pcts, name=ward, mode="lines+markers",
                    line=dict(color=colors.get(ward, "#333")),
                    legendgroup=ward, showlegend=True,
                ), row=1, col=1)
                fig_trend.add_trace(go.Scatter(
                    x=dates, y=answered_pcts, name=ward, mode="lines+markers",
                    line=dict(color=colors.get(ward, "#333")),
                    legendgroup=ward, showlegend=False,
                ), row=1, col=2)
                fig_trend.add_trace(go.Scatter(
                    x=dates, y=gvi1_pcts, name=ward, mode="lines+markers",
                    line=dict(color=colors.get(ward, "#333")),
                    legendgroup=ward, showlegend=False,
                ), row=2, col=1)
                fig_trend.add_trace(go.Scatter(
                    x=dates, y=gvi2_pcts, name=ward, mode="lines+markers",
                    line=dict(color=colors.get(ward, "#333")),
                    legendgroup=ward, showlegend=False,
                ), row=2, col=2)
        fig_trend.update_layout(
            title="Canvassing Progress Over Time",
            height=700, template="plotly_white",
            xaxis=dict(type="category", title="Date"),
            xaxis2=dict(type="category", title="Date"),
            xaxis3=dict(type="category", title="Date"),
            xaxis4=dict(type="category", title="Date"),
        )

    # --- Summary table ---
    table_html = _build_summary_table(stats, wards)

    # --- Assemble HTML ---
    # Order: GVI, Party, Canvassing Status, Summary Table, then Comparison/Capture/GE2024/Trends
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    gvi_party_note = (
        '<p class="chart-note">Note: GVI and Usual Party figures show the most recent '
        'value per voter. TTW\'s ward stats count every canvassing round separately '
        '(including repeat visits), so their totals will be higher than shown here. '
        'Our per-voter approach avoids double-counting and better reflects the current '
        'state of voter intentions.</p>'
    )

    # --- Summary table ---
    dnk_note = (
        '<p class="chart-note"><strong>Notes:</strong><br>'
        '(1) Door counts currently include Do Not Knock (DNK) and No Longer at Address (NLA) '
        'entries to align with TTW\'s ward stats. Since these are not knockable, their '
        'contribution will be removed in a future update.<br>'
        '(2) Our knocked/answered counts include historical canvassing data uploaded from the '
        'previous canvassing app. TTW\'s ward stats only count door knocks recorded through '
        'TTW itself, so our figures will be higher.<br>'
        '(3) The answered % is not terribly informative at present, as many canvassers are not '
        'systematically recording "knocked but no answer" — only successful contacts. This means '
        'the knocked and answered counts are closer than they should be.</p>'
    )
    table_section = f'<div class="chart"><h3>Summary Table</h3>{table_html}{dnk_note}</div>'
    status_section = f'<div class="chart">{fig_status.to_html(full_html=False, include_plotlyjs=False)}</div>'

    # Top row: Summary Table + Canvassing Status side-by-side
    overview_row = (
        f'<div class="chart-row">'
        f'<div class="chart-col">{table_section}</div>'
        f'<div class="chart-col">{status_section}</div>'
        f'</div>'
    )

    # Election-data charts
    party_gvi_ward_table = _build_party_gvi_ward_table(stats, wards, PARTY_ORDER)
    election_charts = ""
    election_charts += f'<div class="chart">{fig_gvi.to_html(full_html=False, include_plotlyjs=False)}{gvi_party_note}</div>'
    election_charts += f'<div class="chart">{fig_party.to_html(full_html=False, include_plotlyjs=False)}{gvi_party_note}</div>'
    election_charts += (
        f'<div class="chart">{fig_party_gvi_wards.to_html(full_html=False, include_plotlyjs=False)}'
        '<p class="chart-note">Per-ward view: each subplot\'s bar for a given party sums to 100% '
        'across the five GVI colours (or is empty if that party has no canvassed voters with a '
        'valid GVI in that ward). Short party labels: Green, Lab, Con, LD, Ref, PC, Ind, RA, Other. '
        'See the chart below for the same data pooled across all wards.</p>'
        '</div>'
    )
    election_charts += f'<div class="chart">{fig_party_gvi.to_html(full_html=False, include_plotlyjs=False)}{gvi_party_note}</div>'
    election_charts += (
        '<div class="chart"><h3>Usual Party &times; GVI breakdown — by ward</h3>'
        f'{party_gvi_ward_table}'
        '<p class="chart-note">Each row shows the GVI distribution for voters in that ward with the given Usual Party. '
        'N = number of voters with both a recorded Usual Party and a valid GVI (1–5). '
        'Percentages sum to 100% across the five GVI columns. Ward/party rows with no data are omitted.<br>'
        '<strong>Note on interpretation:</strong> voters with a recorded Usual Party but no GVI are excluded '
        'from the denominator. This is intentional — the purpose here is to gauge swing percentages, for which '
        'no-GVI entries carry no signal. Any projection from these figures to the wider electorate relies on '
        'extrapolating from the voters we have actually spoken to, and so carries substantial uncertainty, '
        'especially where N is small.</p>'
        f'{low_warn}'
        '</div>'
    )

    # Operational / trend charts
    bottom_charts = ""
    bottom_charts += f'<div class="chart">{fig_ge2024.to_html(full_html=False, include_plotlyjs=False)}</div>'

    if fig_trend:
        bottom_charts += f'<div class="chart">{fig_trend.to_html(full_html=False, include_plotlyjs=False)}{low_warn}</div>'
    else:
        bottom_charts += '<div class="chart"><p style="text-align:center;color:#666;padding:40px;">Trend data will appear after multiple report runs.</p></div>'

    # Postal voter section
    total_postal = sum(s["postal_voters"] for s in stats.values())
    if total_postal < 10:
        bottom_charts += '<div class="chart"><p style="text-align:center;color:#666;padding:40px;">Postal voter data not yet available in the TTW export.</p></div>'

    charts_html = overview_row + election_charts + bottom_charts

    password_js = ""
    if password:
        password_js = f"""
        <script>
        (function() {{
            var p = prompt("Enter password to view this report:");
            if (p !== "{password}") {{
                document.body.innerHTML = '<h1 style="text-align:center;margin-top:100px;color:#e74c3c;">Access denied</h1>';
            }}
        }})();
        </script>
        """

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Brent Greens - Ward Statistics</title>
    <script>{plotly_js}</script>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               max-width: 1400px; margin: 0 auto; padding: 20px; background: #f8f9fa; }}
        h1 {{ color: #27ae60; text-align: center; }}
        .subtitle {{ text-align: center; color: #666; margin-bottom: 30px; }}
        .chart {{ background: white; border-radius: 8px; padding: 15px; margin: 20px 0;
                  box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .chart-row {{ display: flex; gap: 20px; align-items: stretch; flex-wrap: wrap; }}
        .chart-col {{ flex: 1 1 500px; min-width: 0; }}
        .chart-col > .chart {{ margin: 20px 0; height: calc(100% - 40px); }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #27ae60; color: white; padding: 12px 8px; text-align: left; }}
        td {{ padding: 10px 8px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background: #f0f0f0; }}
        tr.ward-header td {{ background: #eafaf1; color: #186a3b; padding-top: 14px; }}
        tr.ward-header:hover td {{ background: #eafaf1; }}
        .note {{ text-align: center; color: #999; font-size: 0.85em; margin-top: 40px; }}
        .chart-note {{ color: #888; font-size: 0.85em; font-style: italic; margin: 10px 15px 0; }}
        .low-answer-warning {{ color: #a53125; background: #fdecea; border-left: 3px solid #c0392b;
                               padding: 8px 12px; margin: 12px 15px 0; font-size: 0.85em;
                               border-radius: 4px; }}
    </style>
    {password_js}
</head>
<body>
    <h1>Brent Greens - Ward Statistics Dashboard</h1>
    <p class="subtitle">Generated: {now_str}</p>

    {charts_html}

    <p class="note">Report generated from TTW Data Export. Historical trends require multiple report runs.</p>
</body>
</html>"""

    return html


def _build_party_gvi_ward_table(stats, wards, party_order):
    """Build an HTML table showing Party x GVI breakdown per ward.

    Each row is a (ward, party) pair. Columns are: N (denominator = voters with
    that party AND a valid GVI 1-5), then GVI 1-5 percentages. Percentages sum
    to 100% across the 5 GVI columns (same convention as fig_party_gvi / fig_gvi).
    Rows where a party has zero voters with valid GVI in that ward are omitted
    to keep the table compact.
    """
    rows = []
    for ward in wards:
        pg = stats[ward].get("party_gvi", {})
        ward_rows = []
        for party in party_order:
            counts = pg.get(party, {})
            denom = sum(counts.get(str(i), 0) for i in range(1, 6))
            if denom == 0:
                continue
            cells = []
            for i in range(1, 6):
                c = counts.get(str(i), 0)
                pct = c / denom * 100
                cells.append(f"<td>{pct:.1f}% ({c})</td>")
            ward_rows.append(
                f"<tr><td>{party}</td><td>{denom}</td>{''.join(cells)}</tr>"
            )
        if ward_rows:
            rows.append(
                f'<tr class="ward-header"><td colspan="7"><strong>{ward}</strong></td></tr>'
            )
            rows.extend(ward_rows)

    if not rows:
        return "<p>No Party x GVI data available.</p>"

    return f"""<table class="pg-table">
        <tr>
            <th>Party</th><th>N</th>
            <th>GVI 1 (Green)</th><th>GVI 2</th><th>GVI 3</th>
            <th>GVI 4</th><th>GVI 5 (Opposition)</th>
        </tr>
        {''.join(rows)}
    </table>"""


def _build_summary_table(stats, wards):
    """Build an HTML summary table matching TTW ward stats format."""
    rows = []
    for ward in wards:
        s = stats[ward]
        total = s["total_doors"]
        knocked_pct = (s["doors_knocked"] / total * 100) if total else 0
        answered_pct = (s["doors_answered"] / total * 100) if total else 0
        gvi_rate = (s["gvi_denominator"] / s["voters_answered"] * 100) if s["voters_answered"] else 0
        remaining = total - s["doors_knocked"]
        rows.append(f"""<tr>
            <td><strong>{ward}</strong></td>
            <td>{total}</td>
            <td>{s['dnk_nla_doors']}</td>
            <td>{s['doors_knocked']} ({knocked_pct:.1f}%)</td>
            <td>{s['doors_answered']} ({answered_pct:.1f}%)</td>
            <td>{gvi_rate:.0f}%</td>
            <td><strong>{remaining}</strong></td>
        </tr>""")

    return f"""<table>
        <tr>
            <th>Ward</th><th>Total Doors</th><th>DNK/NLA</th>
            <th>Knocked (%)</th><th>Answered (%)</th>
            <th>GVI Capture</th><th>Remaining</th>
        </tr>
        {''.join(rows)}
    </table>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate ward statistics dashboard from TTW app-export.")
    parser.add_argument("--input", default=None,
                        help="Path to TTW app-export CSV (default: most recent Brent*.csv in ~/Downloads)")
    parser.add_argument("--password", default=None,
                        help="Password for the HTML report (JS prompt)")
    parser.add_argument("--output", default=str(OUTPUT_PATH),
                        help=f"Output HTML path (default: {OUTPUT_PATH})")
    parser.add_argument("--export-csv", action="store_true",
                        help=f"Also export ward stats and Party x GVI as CSVs "
                             f"into {EXPORTS_DIR}/ (datestamped filenames).")
    args = parser.parse_args()

    # Find input
    input_path = args.input or find_input_csv()
    print(f"Input: {input_path}")

    # Load data
    df = pd.read_csv(input_path, encoding="utf-8-sig", dtype=str)
    df = df.fillna("")
    print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

    # Load ward map
    ward_map = load_ward_map()
    print(f"  Ward map: {len(ward_map)} polling districts across {len(set(ward_map.values()))} wards")

    # Compute stats
    print("  Computing statistics...")
    stats = compute_ward_stats(df, ward_map)

    for ward, s in sorted(stats.items()):
        knocked_pct = (s["doors_knocked"] / s["total_doors"] * 100) if s["total_doors"] else 0
        answered_pct = (s["doors_answered"] / s["total_doors"] * 100) if s["total_doors"] else 0
        print(f"    {ward}: {s['total_doors']} doors, "
              f"{s['doors_knocked']} knocked ({knocked_pct:.1f}%), "
              f"{s['doors_answered']} answered ({answered_pct:.1f}%)")

    # Save snapshot
    snap_path = save_snapshot(stats, input_path, len(df))
    if snap_path:
        print(f"  Snapshot saved: {snap_path}")
    else:
        print(f"  Snapshot skipped (stats unchanged from last snapshot)")

    # Cleanup old snapshots
    removed = cleanup_snapshots()
    if removed:
        print(f"  Cleaned up {removed} old snapshot(s)")

    # Load history
    snapshots = load_snapshots()
    print(f"  Historical snapshots: {len(snapshots)}")

    # Generate HTML
    print("  Generating dashboard...")
    plotly_js = _get_plotly_js()
    html = generate_html(stats, snapshots, password=args.password, plotly_js=plotly_js)
    Path(args.output).write_text(html)
    print(f"  Dashboard written: {args.output}")

    # Optionally export CSVs for sharing with non-technical collaborators
    if args.export_csv:
        stats_path, pg_path = export_csvs(stats, sorted(stats.keys()))
        print(f"  CSV exported: {stats_path}")
        print(f"  CSV exported: {pg_path}")

    print(f"\nOpen in browser: file://{Path(args.output).resolve()}")


if __name__ == "__main__":
    main()
