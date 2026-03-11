#!/usr/bin/env bash
set -u

# Resolve script directory (so it works from any working directory)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Check python3 is available
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found on PATH"; exit 1; }

# --- Argument parsing ---
if [ $# -lt 2 ]; then
    echo "Usage: $0 <DS1_path> <DS3_path> [--historic GE2024] [--future LE2026]"
    echo ""
    echo "  DS1_path   Path to enriched council electoral register CSV"
    echo "  DS3_path   Path to canvassing export CSV"
    echo "  --historic  Historic election name (default: GE2024)"
    echo "  --future    Future election name (default: LE2026)"
    exit 1
fi

DS1="$1"
DS3="$2"
shift 2

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
    echo "ERROR: DS1 file not found: $DS1"
    exit 1
fi

if [ ! -f "$DS3" ]; then
    echo "ERROR: DS3 file not found: $DS3"
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

# --- Step 1: Clean ---
echo "Step 1: Cleaning register..."
if ! python3 "$SCRIPT_DIR/clean_register.py" "$DS1" "$OUTDIR/$BASE.cleaned.csv" \
    --mode register+elections \
    --elections "$HISTORIC" "$FUTURE" \
    --election-types historic future \
    --enriched-columns \
    --report "$OUTDIR/$BASE.cleaned.csv.report.txt"; then
    echo "ERROR: Clean step failed."
    exit 1
fi
echo "Clean step complete."
echo ""

# --- Step 2: Enrich ---
echo "Step 2: Merging canvassing data..."
if ! python3 "$SCRIPT_DIR/enrich_register.py" "$OUTDIR/$BASE.cleaned.csv" "$OUTDIR/$BASE.enriched.csv" \
    --canvassing-export "$DS3" \
    --historic-elections "$HISTORIC" \
    --future-elections "$FUTURE" \
    --report "$OUTDIR/$BASE.enriched.csv.report.txt"; then
    echo "ERROR: Enrich step failed. Cleaned output preserved in: $OUTDIR"
    exit 1
fi
echo "Enrich step complete."
echo ""

# --- Summary ---
echo "=== Pipeline complete ==="
echo "Output folder: $OUTDIR"
echo "  $BASE.cleaned.csv"
echo "  $BASE.cleaned.csv.report.txt"
echo "  $BASE.enriched.csv"
echo "  $BASE.enriched.csv.report.txt"

UNMATCHED="$OUTDIR/$BASE.enriched.unmatched.csv"
if [ -f "$UNMATCHED" ]; then
    echo "  $BASE.enriched.unmatched.csv (unmatched canvassing rows)"
fi
