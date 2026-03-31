#!/usr/bin/env bash
set -u

# Resolve script directory (so it works from any working directory)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Check python3 is available
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found on PATH"; exit 1; }

# --- Argument parsing ---
if [ $# -lt 1 ]; then
    echo "Usage: $0 <DS1_path> [DS3_path] [--clean-only] [--historic GE2024] [--future LE2026]"
    echo ""
    echo "  DS1_path     Path to enriched council electoral register CSV"
    echo "  DS3_path     Path to canvassing export CSV (omit with --clean-only)"
    echo "  --clean-only  Only clean the register (skip enrich + validate)"
    echo "  --historic    Historic election name (default: GE2024)"
    echo "  --future      Future election name (default: LE2026)"
    exit 1
fi

DS1="$1"
shift 1

HISTORIC="GE2024"
FUTURE="LE2026"
CLEAN_ONLY=false
DS3=""

# Consume optional positional DS3 path (first non-flag argument)
if [ $# -gt 0 ] && [[ "$1" != --* ]]; then
    DS3="$1"
    shift 1
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --clean-only)
            CLEAN_ONLY=true
            shift 1
            ;;
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

if [ "$CLEAN_ONLY" = true ]; then
    if [ -n "$DS3" ]; then
        echo "NOTE: --clean-only mode — only cleaning $DS1."
        echo "      DS3 argument ($DS3) will be ignored."
        echo "      To merge/enrich, re-run without --clean-only."
        echo ""
        DS3=""
    fi
else
    if [ -z "$DS3" ]; then
        echo "ERROR: DS3 (canvassing export) path is required for the full pipeline."
        echo "       To clean only, use: $0 $DS1 --clean-only"
        echo "       To merge two registers, use: run_merge_registers.sh"
        exit 1
    fi
    if [ ! -f "$DS3" ]; then
        echo "ERROR: DS3 file not found: $DS3"
        exit 1
    fi
fi

# --- Derive names ---
DS1_DIR="$(cd "$(dirname "$DS1")" && pwd)"
BASE="$(basename "$DS1" .csv)"

# --- Create output folder ---
if [ "$CLEAN_ONLY" = true ]; then
    OUTDIR="$DS1_DIR/Cleaned-$(date +%Y-%m-%d_%H-%M-%S)"
else
    OUTDIR="$DS1_DIR/Cleaned-Merged-$(date +%Y-%m-%d_%H-%M-%S)"
fi
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

if [ "$CLEAN_ONLY" = false ]; then
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

    # --- Step 3: Validate ---
    echo "Step 3: Validating enriched output..."
    VALIDATE_ARGS=("$OUTDIR/$BASE.enriched.csv" --base "$OUTDIR/$BASE.cleaned.csv"
        --report "$OUTDIR/$BASE.enriched.csv.report.txt"
        --elections "$HISTORIC" "$FUTURE" --canvassing-export "$DS3")
    if [ -f "$OUTDIR/$BASE.enriched.unmatched.csv" ]; then
        VALIDATE_ARGS+=(--unmatched "$OUTDIR/$BASE.enriched.unmatched.csv")
    fi
    if ! python3 "$SCRIPT_DIR/validate_enrichment.py" "${VALIDATE_ARGS[@]}"; then
        echo "WARNING: Validation found issues. Review the report above."
        echo "         Enriched output preserved in: $OUTDIR"
    fi
    echo ""
fi

# --- Summary ---
echo "=== Pipeline complete ==="
echo "Output folder: $OUTDIR"
echo "  $BASE.cleaned.csv"
echo "  $BASE.cleaned.csv.report.txt"

if [ "$CLEAN_ONLY" = false ]; then
    echo "  $BASE.enriched.csv"
    echo "  $BASE.enriched.csv.report.txt"

    UNMATCHED="$OUTDIR/$BASE.enriched.unmatched.csv"
    if [ -f "$UNMATCHED" ]; then
        echo "  $BASE.enriched.unmatched.csv (unmatched canvassing rows)"
    fi
fi
