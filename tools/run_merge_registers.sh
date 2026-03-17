#!/usr/bin/env bash
set -u

# Merge two council-format electoral registers, optionally adding canvassing data.
#
# The base register is cleaned to TTW format. The enriched register is passed
# raw to enrich_register.py (which handles council column names and reads the
# original GE24/Party/PostalVoter?/1-5 columns directly). If a canvassing
# export is provided, it is merged in the same enrichment step.
#
# Usage:
#   run_merge_registers.sh <base_register> <enriched_register> [canvassing_export] [options]
#
# Examples:
#   ./run_merge_registers.sh base.csv enriched.csv
#   ./run_merge_registers.sh base.csv enriched.csv canvassing.csv
#   ./run_merge_registers.sh base.csv enriched.csv canvassing.csv --historic GE2024 --future LE2026

# Resolve script directory (so it works from any working directory)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Check python3 is available
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found on PATH"; exit 1; }

# --- Argument parsing ---
if [ $# -lt 2 ]; then
    echo "Usage: $0 <base_register> <enriched_register> [canvassing_export] [--historic GE2024] [--future LE2026]"
    echo ""
    echo "  base_register       Council-format electoral register CSV (the one to enrich)"
    echo "  enriched_register   Council-format register with GE24/Party/1-5/PostalVoter? data"
    echo "  canvassing_export   (Optional) TTW canvassing export CSV (profile_name, address fields)"
    echo "  --historic          Historic election name (default: GE2024)"
    echo "  --future            Future election name (default: LE2026)"
    exit 1
fi

DS1="$1"
DS2="$2"
shift 2

# Optional third positional argument: canvassing export
DS3=""
if [ $# -gt 0 ] && [[ "$1" != --* ]]; then
    DS3="$1"
    shift
fi

HISTORIC="GE2024"
FUTURE="LE2026"

while [ $# -gt 0 ]; do
    case "$1" in
        --historic)
            [ $# -lt 2 ] && { echo "ERROR: --historic requires a value"; exit 1; }
            HISTORIC="$2"
            shift 2
            ;;
        --future)
            [ $# -lt 2 ] && { echo "ERROR: --future requires a value"; exit 1; }
            FUTURE="$2"
            shift 2
            ;;
        *)
            echo "ERROR: Unknown argument: $1"
            exit 1
            ;;
    esac
done

# --- Validation ---
if [ ! -f "$DS1" ]; then
    echo "ERROR: Base register not found: $DS1"
    exit 1
fi

if [ ! -f "$DS2" ]; then
    echo "ERROR: Enriched register not found: $DS2"
    exit 1
fi

if [ -n "$DS3" ] && [ ! -f "$DS3" ]; then
    echo "ERROR: Canvassing export not found: $DS3"
    exit 1
fi

# --- Derive names ---
DS1_DIR="$(cd "$(dirname "$DS1")" && pwd)"
BASE="$(basename "$DS1" .csv)"

# --- Create output folder ---
OUTDIR="$DS1_DIR/Cleaned-Merged-$(date +%Y-%m-%d_%H-%M-%S)"
mkdir "$OUTDIR"
echo "Output folder: $OUTDIR"
echo ""

# --- Step 1: Clean base register ---
echo "Step 1: Cleaning base register..."
if ! python3 "$SCRIPT_DIR/clean_register.py" "$DS1" "$OUTDIR/$BASE.cleaned.csv" \
    --mode register+elections \
    --elections "$HISTORIC" "$FUTURE" \
    --election-types historic future \
    --enriched-columns \
    --report "$OUTDIR/$BASE.cleaned.csv.report.txt"; then
    echo "ERROR: Clean step failed."
    exit 1
fi
echo "Base register cleaned."
echo ""

# --- Step 2: Merge enriched register (and canvassing if provided) ---
echo "Step 2: Merging data into base..."

# Build enrich command — pass raw (uncleaned) enriched register so
# enrich_register.py can read original GE24/Party/PostalVoter?/1-5 columns
ENRICH_ARGS=("$OUTDIR/$BASE.cleaned.csv" "$OUTDIR/$BASE.enriched.csv"
    --enriched-register "$DS2"
    --historic-elections "$HISTORIC"
    --future-elections "$FUTURE"
    --report "$OUTDIR/$BASE.enriched.csv.report.txt")

if [ -n "$DS3" ]; then
    echo "  Including canvassing export: $DS3"
    ENRICH_ARGS+=(--canvassing-export "$DS3")
fi

if ! python3 "$SCRIPT_DIR/enrich_register.py" "${ENRICH_ARGS[@]}"; then
    echo "ERROR: Merge step failed. Cleaned output preserved in: $OUTDIR"
    exit 1
fi
echo "Merge complete."
echo ""

# --- Step 3: Validate ---
echo "Step 3: Validating merged output..."
VALIDATE_ARGS=("$OUTDIR/$BASE.enriched.csv" --base "$OUTDIR/$BASE.cleaned.csv"
    --report "$OUTDIR/$BASE.enriched.csv.report.txt"
    --elections "$HISTORIC" "$FUTURE")
if [ -n "$DS3" ]; then
    VALIDATE_ARGS+=(--canvassing-export "$DS3")
fi
if [ -f "$OUTDIR/$BASE.enriched.unmatched.csv" ]; then
    VALIDATE_ARGS+=(--unmatched "$OUTDIR/$BASE.enriched.unmatched.csv")
fi
if ! python3 "$SCRIPT_DIR/validate_enrichment.py" "${VALIDATE_ARGS[@]}"; then
    echo "WARNING: Validation found issues. Review the report above."
    echo "         Merged output preserved in: $OUTDIR"
fi
echo ""

# --- Summary ---
echo "=== Pipeline complete ==="
echo "Output folder: $OUTDIR"
echo "  $BASE.cleaned.csv              (base, cleaned)"
echo "  $BASE.cleaned.csv.report.txt   (clean report)"
echo "  $BASE.enriched.csv             (final merged output)"
echo "  $BASE.enriched.csv.report.txt  (merge report)"

UNMATCHED="$OUTDIR/$BASE.enriched.unmatched.csv"
if [ -f "$UNMATCHED" ]; then
    echo "  $BASE.enriched.unmatched.csv   (unmatched canvassing rows)"
fi
