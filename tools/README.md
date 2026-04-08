# TTW Digital Electoral Register Tools

Requires Python 3.7+ (standard library only — no additional packages needed).

Tools for converting council electoral register data into the CSV format required by [TTW Digital](https://ttwdigital.co.uk), the Green Party's canvassing platform. Input encoding (UTF-8, Latin-1) is auto-detected.

---

## Quick Start: Clean up council register data

**Run all commands in this README from the repository root directory** (the folder containing `tools/`).

If you just want to clean up a council electoral register CSV for upload to TTW, this is all you need:

```bash
python3 tools/clean_register.py council_data.csv cleaned.csv
```

This will:
- Normalise names (ALL CAPS → Title Case, handle Mc/Mac, apostrophes, hyphens)
- Reformat addresses (flat numbers, building names, zero-pad flats, etc.)
- Standardise postcodes
- Map council column names to TTW column names
- Generate a QA report at `cleaned.csv.report.txt` listing every change and any warnings

Open the QA report and look for `NEEDS MANUAL FIX` warnings — those require editing the output CSV before upload.

See [Address Formatting Rules](#address-formatting-rules) below for the full list of auto-fixes applied.

**For address cleanup only, you can stop here — you don't need any of the other sections.**

For more complex scenarios (merging multiple registers, adding canvassing data, updating an existing TTW export, finding unregistered members), see [Usage](#usage) below.

---

## Tools Overview

### Core conversion tools

| Script | Purpose |
|--------|---------|
| `clean_register.py` | Main converter: council CSV → TTW upload format (address cleanup, names, dates) |
| `enrich_register.py` | Merge canvassing/election data into an existing TTW CSV |
| `validate_enrichment.py` | Verify enrichment didn't corrupt base data |

### App-export update tools

| Script | Purpose |
|--------|---------|
| `update_app_export.py` | Update a TTW app-export CSV with fresh council register data (GVI, party, tags, notes) |
| `validate_app_update.py` | Verify app-export update only modified expected fields |

### Membership check tools

| Script | Purpose |
|--------|---------|
| `check_membership_registration.py` | Cross-check a membership list against the register to find unregistered members |
| `validate_membership_check.py` | Verify membership cross-check output is correct and uncorrupted |

### Pipeline scripts

| Script | Purpose |
|--------|---------|
| `run_pipeline.sh` | Automates clean → enrich → validate for a council register + canvassing data |
| `run_merge_registers.sh` | Automates the three-step merge of two enriched registers |

### Supporting files

| Script | Purpose |
|--------|---------|
| `ttw_common.py` | Shared utilities (I/O, postcode validation, party mapping) |
| `generate_dummy_data.py` | Generate sample test datasets |
| `test_conversion.py` | Test suite for `clean_register.py` |
| `test_enrichment.py` | Test suite for `enrich_register.py` |
| `test_validation.py` | Test suite for `validate_enrichment.py` |
| `test_update_app_export.py` | Test suite for `update_app_export.py` |
| `test_validate_app_update.py` | Test suite for `validate_app_update.py` |
| `test_membership_check.py` | Test suite for `check_membership_registration.py` |
| `test_validate_membership.py` | Test suite for `validate_membership_check.py` |

---

## Before You Upload to TTW

Before uploading your converted CSV, you must complete these steps in the TTW Digital app:

1. **Set up at least one Ward** (see TTW User Guide C1)
2. **Select/add elections** — at least one future election, plus any historic elections you have data for (User Guide C1)
3. **Upload your electoral register** using the output from these tools (User Guide C3)
4. **Assign Polling Districts to Wards** (User Guide C1)

TTW provides a **Sandbox environment** for testing uploads before going live. Data in Sandbox is deleted every night at midnight. We recommend testing your first upload there to check the data parses correctly before uploading to the live service.

You can upload incrementally — as little or as much of the register as you want. The remainder can be uploaded later.

---

## Usage

### 1. Convert council register (register only)

```bash
python3 tools/clean_register.py council_data.csv cleaned.csv
```

### 2. Convert with election data (enriched spreadsheet)

If your input spreadsheet has canvassing columns (GE24, Party, 1-5) alongside the register:

```bash
python3 tools/clean_register.py council_data.csv cleaned.csv \
    --mode register+elections \
    --elections GE2024 LE2026 \
    --election-types historic future \
    --enriched-columns
```

- `--elections`: Name each election (used as column prefix in output)
- `--election-types`: `historic` (generates Voted column) or `future` (generates Green Voting Intention, Party, Postal Voter)
- `--enriched-columns`: Maps GE24 → historic Voted, Party/1-5 → future election columns

### 3. Strip non-TTW columns for upload

```bash
python3 tools/clean_register.py council_data.csv upload_ready.csv \
    --mode register+elections \
    --elections GE2024 LE2026 \
    --election-types historic future \
    --enriched-columns \
    --strip-extra
```

`--strip-extra` removes Email, Phone, DNK, Comments, etc. — keeping only TTW-required columns.

### 4. Enrich an existing TTW CSV with additional data

```bash
python3 tools/enrich_register.py base_ttw.csv enriched_output.csv \
    --enriched-register spreadsheet_with_history.csv \
    --canvassing-export ttw_canvassing_download.csv \
    --canvassing-register new_canvassing_data.csv \
    --historic-elections GE2024 \
    --future-elections LE2026
```

### 5. Merge two enriched register datasets

When you have two versions of the electoral register with enrichment data — e.g. a primary register (Dataset 1) and a second register with additional canvassing data (Dataset 2) — use a three-step process:

**Dataset 1** (primary): Full council register with enrichment columns (GE24, Party, 1-5, Email, Phone, Comments, etc.)

**Dataset 2** (additional): A second enriched register, possibly with extra fields like `PostalVoter?` or updated Party/1-5 data.

```bash
# Step 1: Convert Dataset 1 to TTW format (this becomes the base)
python3 tools/clean_register.py dataset1.csv base_ttw.csv \
    --mode register+elections \
    --elections GE2024 LE2026 \
    --election-types historic future \
    --enriched-columns

# Step 2: Merge Dataset 2's historic election data + extra columns
#   - GE24         -> GE2024 Voted
#   - Party        -> GE2024 Party
#   - PostalVoter? -> LE2026 Postal Voter
#   - Email, Phone, Comments, Issues, DNK, New, 1st round -> extra columns
python3 tools/enrich_register.py base_ttw.csv enriched.csv \
    --enriched-register dataset2.csv \
    --historic-elections GE2024 \
    --future-elections LE2026

# Step 3: Merge Dataset 2's future election data (second pass)
#   - 1-5   -> LE2026 Green Voting Intention
#   - Party -> LE2026 Party
python3 tools/enrich_register.py enriched.csv final.csv \
    --canvassing-register dataset2.csv \
    --future-elections LE2026
```

**Why two passes?** The `--enriched-register` flag handles historic election columns (GE24 → Voted, Party → historic Party) and extra metadata columns, but does not map `1-5` to future Green Voting Intention. The `--canvassing-register` flag handles future election columns (Party → future Party, 1-5 → GVI). Running both passes ensures all data from Dataset 2 is merged.

Both passes use fuzzy matching on name + postcode to match rows between datasets. Review the QA reports from each step to check match quality.

### 6. Validate enrichment

```bash
python3 tools/validate_enrichment.py final.csv \
    --base base_ttw.csv \
    --elections GE2024 LE2026
```

Exit code 0 = passed, 1 = failed. Use `--strict` to promote warnings to failures.

### 7. Automated pipelines (shell scripts)

For the common workflows, two shell scripts automate the multi-step processes:

**`run_pipeline.sh`** — clean a register, optionally enrich with canvassing data, and validate:

```bash
# Clean register only
./tools/run_pipeline.sh council_register.csv --clean-only

# Clean + enrich with canvassing data
./tools/run_pipeline.sh council_register.csv canvassing_export.csv

# Custom election names
./tools/run_pipeline.sh council_register.csv canvassing_export.csv \
    --historic GE2024 --future LE2026
```

Output goes to a timestamped folder (e.g. `Cleaned-2026-04-08_14-23-45/` or `Cleaned-Merged-2026-04-08_14-23-45/`).

**`run_merge_registers.sh`** — merge two council-format registers (primary + additional enrichment data), optionally with canvassing data:

```bash
# Merge two registers
./tools/run_merge_registers.sh base_register.csv enriched_register.csv

# Merge two registers + canvassing export
./tools/run_merge_registers.sh base_register.csv enriched_register.csv canvassing_export.csv

# Custom election names
./tools/run_merge_registers.sh base_register.csv enriched_register.csv \
    --historic GE2024 --future LE2026
```

This replaces the manual three-step process in section 5.

### 8. Update an existing TTW app-export with fresh council data

If you already have voter records in TTW and want to apply fresh canvassing data (GVI, party, postal voter, notes, tags) from a council register:

```bash
python3 tools/update_app_export.py app_export.csv register.csv updated.csv
```

Options:
- `--changed-only` — output only rows that were actually modified (useful for a smaller re-upload)
- `--date YYYY-MMM-DD` — override today's date for note timestamps (default: today)
- `--match-threshold N` — fuzzy match threshold (default: 0.8)

The script produces four output files:
- `updated.csv` — the updated app-export (or only changed rows with `--changed-only`)
- `updated.rejects2check.csv` — register rows with ambiguous, duplicate, or borderline matches (need manual review)
- `updated.unmatched.csv` — register rows with no match in the app-export (moved away / not registered)
- `updated.csv.report.txt` — QA report

**Then validate the update:**

```bash
python3 tools/validate_app_update.py app_export.csv updated.csv
```

Add `--changed-only` if you used that flag on the update. This verifies that only amendable fields were modified — protected fields (Voter UUID, name, address) must be identical to the original.

### 9. Find unregistered members

Cross-check a Green Party membership list against the electoral register to find members who are not registered to vote:

```bash
python3 tools/check_membership_registration.py membership.csv register.csv unregistered.csv
```

Options:
- `--strict` — exclude borderline possible matches from the output
- `--match-threshold N` — fuzzy match threshold (default: 0.8)

Output includes a `Match_Status` column showing why each row is in the output (`unmatched`, `out_of_area`, `no_postcode`, `ambiguous`, `possible`).

**Then validate:**

```bash
python3 tools/validate_membership_check.py membership.csv register.csv unregistered.csv
```

### `clean_register.py` options

| Flag | Description |
|------|-------------|
| `--mode` | `register` (default) or `register+elections` |
| `--elections` | Election names (used as column prefixes when mode is `register+elections`) |
| `--election-types` | Per election: `historic` or `future` (same order as `--elections`) |
| `--enriched-columns` | Map GE24→historic Voted, Party/1-5→future election columns |
| `--strip-extra` | Remove non-TTW extra columns (Email, Phone, DNK, etc.) from output |
| `--strip-empty` | Remove entirely-empty optional columns (Address3-6, UPRN, etc.) |
| `--date-format DMY` | Input date format hint: `DMY` (default), `YMD`, or `MDY` |
| `--max-rows N` | Warn if input exceeds N rows (default: 100,000) |
| `--report PATH` | Custom path for the QA report (default: `OUTPUT.report.txt`) |
| `--quiet` | Suppress progress output |

### `enrich_register.py` options

| Flag | Description |
|------|-------------|
| `--match-threshold N` | Fuzzy match threshold for name matching (default: 0.8) |
| `--strip-extra` | Remove non-TTW extra columns from output |
| `--dry-run` | Preview matches without writing output |
| `--report PATH` | Custom path for the QA report |
| `--quiet` | Suppress progress output |

### `validate_enrichment.py` options

| Flag | Description |
|------|-------------|
| `--base PATH` | **(Required)** Base TTW CSV to compare against |
| `--report PATH` | Custom path for the validation report |
| `--min-match-rate N` | Minimum proportion of rows that must match (default: 0.7) |
| `--strict` | Promote warnings to failures (affects exit code) |
| `--quiet` | Suppress progress output |

---

## What the Converter Does

The converter applies the following transformations automatically:

### Column mapping
- Maps council column names (PDCode, RollNo, ElectorForename, etc.) to TTW column names (Elector No. Prefix, Elector No., Forename, etc.)
- Discards council-only columns (ElectorTitle, IERStatus, FranchiseMarker, Ward, Parl, Euro, etc.)
- Preserves SubHouse/House columns as pass-through council data

### Name normalisation
- ALL CAPS and all lowercase names are title-cased (e.g. `SMITH` → `Smith`)
- Handles hyphens (`O'BRIEN-SMYTHE` → `O'Brien-Smythe`), apostrophes, Mc/Mac prefixes
- Mixed-case names (e.g. `McDonald`) are left untouched

### Address reformatting
See [Address Formatting Rules](#address-formatting-rules) below for full details.

### Elector number / suffix handling
- **Decimal RollNos** (e.g. 3.5, 10.75): Split into integer Elector No. + sequential suffix (0, 1, 2...). Only groups containing at least one decimal get renumbered.
- **Existing Suffix column**: Used as-is when no decimal RollNos are present.
- **No decimals, no Suffix column**: All rows get suffix "0".
- **Duplicate Full Elector No.**: Automatically resolved by reassigning suffixes sequentially.

### Date normalisation
- Parses multiple formats (DD/MM/YYYY, YYYY-MM-DD, DD-MM-YYYY, DD.MM.YYYY)
- Outputs standard DD/MM/YYYY
- Date of Attainment is only included in output if the input data contains date values

### Postcode normalisation
- Uppercased and formatted with standard spacing (e.g. `nw10 3ju` → `NW10 3JU`)

### Party / voting intention mapping
- Party names mapped to TTW codes: Green→G, Labour→Lab, Conservative→Con, Liberal Democrat→LD, Reform→REF
- Additional valid TTW party codes: `L`, `PC`, `RA`, `Ind`, `Oth`
- Voting intention 1-5 validated (out-of-range values warned)
- "Won't say", "Did not vote", "Don't know", "Refused to say", "No answer", blank → left empty

### Deletion
- Rows with no address data at all are deleted (required by TTW)

### Output format
- UTF-8 with BOM (required by TTW)
- CRLF line endings
- QA report generated alongside output

---

## Address Formatting Rules

TTW Digital requires addresses to follow specific formatting for correct parsing. The converter auto-fixes what it can and flags the rest for manual review.

### Flat / unit designations must be in Address1

The flat, unit, apartment, or room number must appear in **Address1**, with the street address in **Address2**.

| Correct | Incorrect |
|---------|-----------|
| Address1: `Flat 3` / Address2: `30 Chamberlayne Road` | Address1: `Flat 3, 30 Chamberlayne Road` (all in one field) |
| Address1: `Flat 3` / Address2: `30 Chamberlayne Road` | Address1: `30 Flat 3` / Address2: `Chamberlayne Road` (number before flat) |

**Auto-fixes applied:**
- `Flat 3, 30 Chamberlayne Road` → split into Address1: `Flat 3` / Address2: `30 Chamberlayne Road`
- `Flat 3 30 Chamberlayne Road` (no comma) → split the same way when the remainder looks like a road
- `56 Flat 1` → reordered to Address1: `Flat 1` / Address2: `56` (or prepended to existing Address2)

### Dual-number addresses use bracket notation

When an address has two numbers (e.g. a building number and a street number), the first number must be in square brackets so TTW knows it's not the house number.

| Correct | Incorrect |
|---------|-----------|
| `[506], 10 Evelina Gardens` | `506, 10 Evelina Gardens` |
| `[506], 10 Evelina Gardens` | `506 10 Evelina Gardens` |
| Address1: `[506]` / Address2: `10 Evelina Gardens` | Address1: `506` / Address2: `10 Evelina Gardens` |

**Auto-fixes applied:**
- `506, 10 Evelina Gardens` → `[506], 10 Evelina Gardens`
- `506 10 Evelina Gardens` → `[506], 10 Evelina Gardens`
- Address1 bare `506` with Address2 starting with digit → Address1: `[506]`
- Already-bracketed addresses (e.g. `[506], 10 Evelina Gardens`) are left alone

### Building names go before the number

When Address1 has a house number followed by a building name (not a road), it's reordered.

| Correct | Incorrect |
|---------|-----------|
| `Ontario Point 10` | `10 Ontario Point` |

### Flat numbers are zero-padded for sort order

Within each building, flat numbers are padded to consistent width so TTW sorts them correctly.

| Before | After | Why |
|--------|-------|-----|
| `Flat 1` | `Flat 01` | Building has `Flat 12` (2-digit max) |
| `Flat 3A` | `Flat 03A` | Same building |
| `Flat 12` | `Flat 12` | Already at max width |
| `Flat A` | `Flat A` | Non-numeric — unchanged |
| `Flat Ground Floor` | `Flat Ground Floor` | Multi-word — unchanged |

### Address gaps are shifted up

If Address2 is empty but Address3 has data, values are shifted up to fill gaps.

### Flagged for manual review

These patterns are too ambiguous to auto-fix and generate `NEEDS MANUAL FIX` warnings in the QA report:

- **Ampersands in Address1** — e.g. `Maisonette (Ground & 1st Floor Front)`. Split the flat/unit description into Address1 and the street into Address2 manually.
- **Commas in Address1** that aren't a `Flat X, Road` pattern — may need manual splitting into Address1 / Address2.
- **Long flat designations** (5+ words, no comma) — e.g. `Flat Ground Floor 30 Chamberlayne Road`. May need manual splitting.
- **Number before flat with Address2 already having a house number** — ambiguous, needs human judgement.

---

## QA Report

Every run generates a QA report (`OUTPUT.report.txt`) containing:

- **Header**: Input/output files, mode, date, encoding detected
- **Column mapping**: Shows which council column mapped to which TTW column
- **Fixes applied**: Every auto-correction with row number, field, old value, new value, and reason
- **Warnings**: Issues that need manual attention (duplicate electors, unparseable addresses, invalid data)
- **Deletions**: Rows removed (no-address records) with elector details
- **Summary**: Total counts for input rows, output rows, fixes, warnings, deletions

Always review the QA report after conversion, paying particular attention to:
- `NEEDS MANUAL FIX` warnings — these require editing the output CSV before upload
- Name case changes — verify Mc/Mac names were handled correctly
- Duplicate elector warnings

---

## Known Issues

1. **Dedup grouping may create secondary collisions** — The duplicate Full Elector No. resolver groups by Full Elector No. rather than by (prefix, number), which could theoretically create new collisions when reassigning suffixes. Step 14 (uniqueness check) catches this and aborts with an error. See [GitHub issue #1](https://github.com/catherinewatkinson/ttw-data-prepper/issues/1).

2. **Ampersand addresses require manual fixing** — Addresses containing `&` (e.g. `Ground & 1st Floor`) cannot be auto-parsed. The QA report flags these.

3. **Mac/Mc name heuristic** — Non-Scottish names starting with "Mac" (e.g. "MACKEREL") would be incorrectly title-cased as "MacKerel". All name changes are logged in the QA report for review.

---

## Running Tests

Run all tests:

```bash
python3 -m pytest tools/ -v
```

Or run individual test files:

```bash
python3 tools/test_conversion.py -v
python3 tools/test_enrichment.py -v
python3 tools/test_validation.py -v
python3 tools/test_update_app_export.py -v
python3 tools/test_validate_app_update.py -v
python3 tools/test_membership_check.py -v
python3 tools/test_validate_membership.py -v
```

---

## Data Preparation Guide

*Summary of key points from the Green Party D4 data instructions.*

### Setting up your spreadsheet

Your council will provide the electoral register as a CSV or XLS file. The columns vary by council but typically include:

- **Polling district / ward prefix** — e.g. `LG01`. May be one or two columns. This becomes the Elector No. Prefix.
- **Elector number** — unique number per voter within the polling district.
- **Elector number suffix** — used when voters are added between existing numbers (e.g. between 100 and 101, the new voter is 100/1). Some councils use decimal notation instead (e.g. 100.5).
- **Elector markers** — franchise codes (e.g. `G` for EU nationals who can only vote in certain elections). Generally ignorable for local elections.
- **Date of attainment** — shown for voters turning 18 that year.
- **Name** — may be one column or split into forename/surname. Some councils use ALL CAPS.
- **Address** — typically spread across up to 6 columns plus postcode. Note: some councils include two postcode columns — delete the first one (before the address) and keep the one after.

### Recommended additional columns

If you maintain your own enriched spreadsheet alongside the council register, consider adding:

| Column | Purpose | Example values |
|--------|---------|----------------|
| Marked register (per election) | Who voted | `V` = voted, `VA` = voted by post, `A` = postal vote not returned, blank = didn't vote |
| `New` | When someone moved in | `Feb22` |
| `P/PB` | Poster/posterboard history | `P18` = poster in 2018, `PB22` = posterboard in 2022 |
| `DNK` | Do not knock | `1` = do not knock (e.g. opposition councillors) |
| `Party` | Current party loyalty | `G`, `Lab`, `Con`, `LD`, etc. |
| `1-5` | Green voting intention | 1 = definite Green, 5 = definite opposition |
| `Comments` | Brief notes only | Keep to genuinely useful info (e.g. "hard of hearing, knock loudly") |
| `Email Address` | Contact email | |
| `Phone number` | Contact phone | |

### Keeping the register up to date

- The council publishes a **new full register every December** with renumbered electors. You'll need to migrate your canvassing data to the new register.
- **Monthly updates** are available from the council for additions/removals between annual registers. There are no updates in autumn during the annual canvass period.
- **Before each election period**, update your register to exclude deceased voters and ensure accuracy.

### Using your data effectively

With voting intention and marked register data, you can generate targeted lists:

- **Canvass lists**: Houses with potential voters (voted previously or newly registered), excluding DNK households
- **Postal voter lists**: Filter by postal vote status to knock before postal votes arrive
- **Squeeze leaflets**: Target supporters of third/fourth-place parties
- **Knock-up lists**: Election day — voters with intention 1-2 who haven't voted yet
- **Second-round canvass**: Exclude houses where you've already spoken to someone

### Data protection

- Minimise access — only ward data managers and data entry volunteers who've signed a data protection agreement
- Canvass sheets given to volunteers should contain **doors to knock only** — no personal data. If lost, this avoids a data protection breach.
- Use data validation in Excel/Sheets to lock columns to permitted values (e.g. Party column only accepts `G, Lab, Con, LD, etc.`)

### Useful Excel tips from the D4 guide

- **Fix ALL CAPS surnames**: `=PROPER(A2)` converts to title case
- **Remove extra spaces**: `=TRIM(A2)` strips leading/trailing/double spaces
- **Count leaflets needed**: Filter your target list, copy addresses to a new sheet, use Remove Duplicates → row count = leaflets needed
- **Lock inputs**: Data → Data Validation → List, then enter allowed codes (e.g. `G,Lab,Con,LD,REF`) to prevent freeform entry errors
