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
OUTPUT_PATH = SCRIPT_DIR / "report.html"

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
# HTML generation
# ---------------------------------------------------------------------------

def generate_html(stats, snapshots, password=None):
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
    _bar_xaxis = dict(tickangle=-30)
    fig_status.update_layout(
        title="Canvassing Status by Ward",
        barmode="stack", yaxis_title="% of total doors",
        height=500, template="plotly_white", xaxis=_bar_xaxis,
    )

    # --- Chart: Cross-ward comparison (renamed metrics) ---
    fig_comparison = go.Figure()
    fig_comparison.add_trace(go.Bar(
        name="Coverage (knocked / total doors)", x=[m["ward"] for m in metrics],
        y=[m["knocked_pct"] for m in metrics],
        text=[f"{m['knocked_pct']:.1f}%" for m in metrics],
        textposition="auto",
        hovertemplate="%{x}<br>Coverage: %{y:.1f}% of total doors<extra></extra>",
    ))
    fig_comparison.add_trace(go.Bar(
        name="Contact Rate (answered / total doors)", x=[m["ward"] for m in metrics],
        y=[m["answered_pct"] for m in metrics],
        text=[f"{m['answered_pct']:.1f}%" for m in metrics],
        textposition="auto",
        hovertemplate="%{x}<br>Contact Rate: %{y:.1f}% of total doors<extra></extra>",
    ))
    fig_comparison.add_trace(go.Bar(
        name="Door Open Rate (answered / knocked)", x=[m["ward"] for m in metrics],
        y=[m["answer_rate"] for m in metrics],
        text=[f"{m['answer_rate']:.1f}%" for m in metrics],
        textposition="auto",
        hovertemplate="%{x}<br>Door Open Rate: %{y:.1f}% of knocked doors<extra></extra>",
    ))
    fig_comparison.update_layout(
        title="Cross-Ward Canvassing Rates",
        barmode="group", yaxis_title="Percentage",
        height=500, template="plotly_white", xaxis=_bar_xaxis,
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
        fig_gvi.add_trace(go.Bar(
            name=label, x=wards, y=vals,
            marker_color=color,
            hovertext=hover, hoverinfo="text",
        ))
    fig_gvi.update_layout(
        title="Green Voting Intention Breakdown by Ward",
        barmode="stack", yaxis_title="% of canvassed voters",
        height=500, template="plotly_white", xaxis=_bar_xaxis,
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
        fig_party.add_trace(go.Bar(
            name=party, x=wards, y=vals,
            marker_color=party_colors.get(party, "#999"),
            hovertext=hover, hoverinfo="text",
        ))
    fig_party.update_layout(
        title="Usual Party Breakdown by Ward",
        barmode="stack", yaxis_title="% of voters with party data",
        height=500, template="plotly_white", xaxis=_bar_xaxis,
    )

    # --- Chart 5: GVI capture rate ---
    fig_capture = go.Figure()
    capture_vals = []
    for ward in wards:
        s = stats[ward]
        rate = (s["gvi_denominator"] / s["voters_answered"] * 100) if s["voters_answered"] else 0
        capture_vals.append(rate)
    fig_capture.add_trace(go.Bar(
        x=wards, y=capture_vals,
        text=[f"{v:.0f}%" for v in capture_vals],
        textposition="auto",
        marker_color=["#e74c3c" if v < 50 else "#27ae60" for v in capture_vals],
        hovertemplate="%{x}<br>GVI Capture Rate: %{y:.1f}%<extra></extra>",
    ))
    fig_capture.update_layout(
        title="GVI Capture Rate (GVI recorded / voters answered)",
        yaxis_title="Percentage", height=500, template="plotly_white",
        xaxis=_bar_xaxis,
    )

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

    # Top section: election data charts
    top_charts = ""
    top_charts += f'<div class="chart">{fig_gvi.to_html(full_html=False, include_plotlyjs=False)}{gvi_party_note}</div>'
    top_charts += f'<div class="chart">{fig_party.to_html(full_html=False, include_plotlyjs=False)}{gvi_party_note}</div>'
    top_charts += f'<div class="chart">{fig_status.to_html(full_html=False, include_plotlyjs=False)}</div>'

    # Summary table
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

    # Below table: comparison and operational charts
    bottom_charts = ""
    bottom_charts += f'<div class="chart">{fig_capture.to_html(full_html=False, include_plotlyjs=False)}</div>'
    bottom_charts += f'<div class="chart">{fig_ge2024.to_html(full_html=False, include_plotlyjs=False)}</div>'

    if fig_trend:
        bottom_charts += f'<div class="chart">{fig_trend.to_html(full_html=False, include_plotlyjs=False)}</div>'
    else:
        bottom_charts += '<div class="chart"><p style="text-align:center;color:#666;padding:40px;">Trend data will appear after multiple report runs.</p></div>'

    # Postal voter section
    total_postal = sum(s["postal_voters"] for s in stats.values())
    if total_postal < 10:
        bottom_charts += '<div class="chart"><p style="text-align:center;color:#666;padding:40px;">Postal voter data not yet available in the TTW export.</p></div>'

    charts_html = top_charts + table_section + bottom_charts

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
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               max-width: 1200px; margin: 0 auto; padding: 20px; background: #f8f9fa; }}
        h1 {{ color: #27ae60; text-align: center; }}
        .subtitle {{ text-align: center; color: #666; margin-bottom: 30px; }}
        .chart {{ background: white; border-radius: 8px; padding: 15px; margin: 20px 0;
                  box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #27ae60; color: white; padding: 12px 8px; text-align: left; }}
        td {{ padding: 10px 8px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background: #f0f0f0; }}
        .note {{ text-align: center; color: #999; font-size: 0.85em; margin-top: 40px; }}
        .chart-note {{ color: #888; font-size: 0.85em; font-style: italic; margin: 10px 15px 0; }}
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


def _build_summary_table(stats, wards):
    """Build an HTML summary table matching TTW ward stats format."""
    rows = []
    for ward in wards:
        s = stats[ward]
        total = s["total_doors"]
        knocked_pct = (s["doors_knocked"] / total * 100) if total else 0
        answered_pct = (s["doors_answered"] / total * 100) if total else 0
        answer_rate = (s["doors_answered"] / s["doors_knocked"] * 100) if s["doors_knocked"] else 0
        gvi_rate = (s["gvi_denominator"] / s["voters_answered"] * 100) if s["voters_answered"] else 0
        remaining = total - s["doors_knocked"]
        rows.append(f"""<tr>
            <td><strong>{ward}</strong></td>
            <td>{total}</td>
            <td>{s['dnk_nla_doors']}</td>
            <td>{s['doors_knocked']} ({knocked_pct:.1f}%)</td>
            <td>{s['doors_answered']} ({answered_pct:.1f}%)</td>
            <td>{answer_rate:.0f}%</td>
            <td>{gvi_rate:.0f}%</td>
            <td><strong>{remaining}</strong></td>
        </tr>""")

    return f"""<table>
        <tr>
            <th>Ward</th><th>Total Doors</th><th>DNK/NLA</th>
            <th>Knocked (%)</th><th>Answered (%)</th>
            <th>Answer Rate</th><th>GVI Capture</th><th>Remaining</th>
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
    html = generate_html(stats, snapshots, password=args.password)
    Path(args.output).write_text(html)
    print(f"  Dashboard written: {args.output}")
    print(f"\nOpen in browser: file://{Path(args.output).resolve()}")


if __name__ == "__main__":
    main()
