#!/usr/bin/env python3
"""Shared utilities for TTW Digital electoral register tools.

Functions and constants used by both clean_register.py and enrich_register.py.
"""

import csv
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# UK postcode regex (loose)
UK_POSTCODE_RE = re.compile(r"^[A-Z]{1,2}[0-9][0-9A-Z]?\s[0-9][A-Z]{2}$")

# Valid party codes (includes 'L' as alternate Labour abbreviation per TTW test data)
VALID_PARTY_CODES = {"G", "Con", "Lab", "L", "LD", "REF", "PC", "RA", "Ind", "Oth"}

# Party name -> TTW code mapping (case-insensitive)
PARTY_NAME_MAP = {
    "green party": "G",
    "green": "G",
    "greens": "G",
    "labour": "Lab",
    "conservatives": "Con",
    "conservative": "Con",
    "tory": "Con",
    "tories": "Con",
    "liberal democrats": "LD",
    "liberal democrat": "LD",
    "lib dem": "LD",
    "lib dems": "LD",
    "reform": "REF",
    "reform uk": "REF",
    "reform party": "REF",
    "independent": "Ind",
    "other party": "Oth",
    "other": "Oth",
}

# Values that map to blank (not a party)
PARTY_BLANK_VALUES = {
    "did not vote", "none", "refused to say", "won't say",
    "dont know", "don't know", "no answer",
}


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

def read_input(input_path):
    """Read council CSV, auto-detecting encoding. Returns (rows, encoding, headers)."""
    path = Path(input_path)

    for enc in ["utf-8-sig", "latin-1"]:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                rows = list(reader)
            return rows, enc, headers
        except (UnicodeDecodeError, UnicodeError):
            continue

    print(f"ERROR: Cannot decode {input_path} as UTF-8 or Latin-1.", file=sys.stderr)
    sys.exit(1)


def write_output(rows, headers, output_path):
    """Write TTW-format CSV with UTF-8 BOM and CRLF line endings."""
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\r\n",
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# PostCode normalization
# ---------------------------------------------------------------------------

def normalize_postcode(value):
    """Strip, uppercase, normalize spacing. Returns (normalized, warning_or_None)."""
    if not value or not value.strip():
        return "", None

    pc = " ".join(value.upper().split())  # collapse whitespace, uppercase

    # Insert space before last 3 chars if missing
    if len(pc) >= 5 and " " not in pc:
        pc = pc[:-3] + " " + pc[-3:]

    # Normalize to single space before inward code
    parts = pc.rsplit(" ", 1)
    if len(parts) == 2:
        pc = parts[0].strip() + " " + parts[1].strip()

    if not UK_POSTCODE_RE.match(pc):
        return pc, f"PostCode '{pc}' may not be valid UK format"

    return pc, None


# ---------------------------------------------------------------------------
# Party name mapping
# ---------------------------------------------------------------------------

def map_party_name(value):
    """Map a party name to TTW code. Returns (mapped_value, warning_or_None)."""
    if not value or not value.strip():
        return "", None

    raw = value.strip()

    # Already a valid TTW code? Passthrough.
    if raw in VALID_PARTY_CODES:
        return raw, None

    normalized = raw.replace("_", " ").lower().strip()

    if normalized in PARTY_BLANK_VALUES:
        return "", None

    if normalized in PARTY_NAME_MAP:
        return PARTY_NAME_MAP[normalized], None

    # Unrecognized — keep as-is, warn
    return raw, f"Unrecognized party value '{raw}', kept as-is"
