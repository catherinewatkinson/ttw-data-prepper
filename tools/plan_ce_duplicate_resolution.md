# Plan: CE duplicate resolution by timestamp + fill gaps + new columns

## Context

Canvassing export (CE) duplicates (multiple visits to the same person) are currently resolved by "last row in the file wins" — the code assumes rows are chronologically ordered, which is fragile. The real CE data has a `visit_visited_at` timestamp column (e.g. `2025-06-24 14:00:53`). We should use this to pick the **most recent visit** as primary, then **fill gaps** from older visits (mirroring the ER merge strategy, but with newest-first priority).

Additionally, the CE data now has columns not currently handled: `visit_attempt_status`, `visit_result_status`, `1-5` (Green Voting Intention), and `Comments`.

## Files to modify

- `tools/enrich_register.py` — duplicate resolution, new column support, report updates
- `tools/test_enrichment.py` — tests for timestamp-based ordering, gap-filling, new columns

## Design

### 1. New function: `_merge_ce_rows(primary, secondary, display_name, report)`

Similar to `_merge_er_rows` but for CE. Most recent visit is primary (its values win clashes):

```python
def _merge_ce_rows(primary, secondary, display_name, report):
    """Fill gaps in primary CE row from secondary (older visit). Log clashes."""
    for key in secondary:
        sec_val = secondary[key].strip() if secondary[key] else ""
        if not sec_val:
            continue
        pri_val = primary.get(key, "").strip() if primary.get(key) else ""
        if not pri_val:
            primary[key] = secondary[key]
            report.ce_merge_count += 1
        elif pri_val != sec_val:
            report.ce_merge_clashes.append(
                (display_name, key, pri_val, sec_val))
```

### 2. Change duplicate resolution in `match_canvassing_export` (lines 829-838)

Currently picks `ce_indices[-1]` (last in file). New logic:

```python
for base_idx, ce_indices in base_match_count.items():
    if len(ce_indices) > 1:
        base_name = f"{base_rows[base_idx].get('Forename', '')} {base_rows[base_idx].get('Surname', '')}".strip()

        # Sort by visit_visited_at descending (most recent first)
        # Rows without timestamps sort after rows with timestamps
        def _visit_sort_key(idx):
            ts = ce_rows[idx].get("visit_visited_at", "").strip()
            return ts if ts else ""  # empty sorts before any date string

        sorted_indices = sorted(ce_indices, key=_visit_sort_key, reverse=True)

        # Most recent is primary; fill gaps from older visits
        primary_row = dict(ce_rows[sorted_indices[0]])  # copy to avoid mutating input
        for older_idx in sorted_indices[1:]:
            _merge_ce_rows(primary_row, ce_rows[older_idx], base_name, report)

        matched[base_idx] = primary_row
        report.ce_duplicate_visits.append((base_name, len(ce_indices)))
        report.warnings.append(
            f"Canvassing: {len(ce_indices)} visits matched base row \"{base_name}\", "
            f"most recent used, gaps filled from older visits")
```

**Key detail**: ISO-format timestamps (`2025-06-24 14:00:53`) sort correctly as strings, so no datetime parsing needed. Empty timestamps sort to the end (least recent), preserving backward compatibility when `visit_visited_at` is absent.

### 3. New CE columns

Add to `EXTRA_COLS_CANVASSING` (line 46):

```python
EXTRA_COLS_CANVASSING = ["visit_issues", "visit_notes", "visit_attempt_status", "visit_result_status"]
```

**`1-5` (GVI) handling** — map in `generate_election_columns` for future elections, mirroring the canvassing register pattern (lines 1125-1133):

In the future elections loop (around line 910), after the existing postal voter logic, add:

```python
# 1-5 from CE -> Green Voting Intention (future elections only)
if ce_match:
    ce_gvi_raw = ce_match.get("1-5", "").strip()
    if ce_gvi_raw in {"1", "2", "3", "4", "5"}:
        _set_field(row, gvi_key, ce_gvi_raw, row_key, report)
    elif ce_gvi_raw:
        report.warnings.append(
            f"Canvassing export row {row_key}: invalid 1-5 value '{ce_gvi_raw}', skipped")
```

**`Comments` handling** — add to `add_extra_columns` for CE, alongside existing `visit_issues`/`visit_notes`:

```python
# Comments from CE (if present in CE headers)
if "Comments" in ce_match:
    comments_val = ce_match.get("Comments", "").strip()
    _set_field(row, "Comments", comments_val, row_key, report)
```

This reuses the existing `Comments` column (shared with ER). `_set_field` handles overwrite protection — if ER already set Comments, CE will overwrite it (canvassing is more recent), which matches the existing priority (CE overwrites ER for extra columns).

### 4. Header construction update

In `build_enrichment_headers`, `EXTRA_COLS_CANVASSING` already gets iterated (line 1001-1003), so `visit_attempt_status` and `visit_result_status` are automatically included. No change needed there.

For `Comments`: already added by ER path (`EXTRA_COLS_REGISTER`). If CE-only (no ER), Comments needs adding. Simplest: add "Comments" to the CE header building conditionally (similar to how DNK is handled).

### 5. Report changes

Add to `EnrichQAReport.__init__`:
```python
self.ce_merge_clashes = []  # [(name, field, kept_val, discarded_val)]
self.ce_merge_count = 0     # number of CE fields gap-filled from older visits
```

Add to report `write()` in the CE section:
- Show merge count: `"  Merged (gap fills from older visits): {ce_merge_count}"`
- Show clashes if any
- Update duplicate visits text from "last used" to "most recent used, gaps filled"

Machine-readable section:
```
MERGE_CLASH_CE|Name=...|Field=...|Kept=...|Discarded=...
```

### What stays the same

- Matching logic (name+address scoring) — unchanged
- `base_match_count` tracking — unchanged
- ER merge logic — unchanged (already implemented)
- `_set_field` overwrite protection — unchanged
- Historic election columns from CE: just `visit_previously_voted_for` -> Party (unchanged)
- `1-5` only maps to **future** elections (like canvassing register does)

### Edge cases

- **No `visit_visited_at` column**: All timestamps are empty -> string sort puts them all equal -> falls back to original file order (last in file wins). Backward compatible.
- **Mixed timestamps**: Rows with timestamps sort before rows without. Most recent timestamped row is primary.
- **Single CE match**: No merging needed — same as before.

## Tests to add

1. `test_ce_duplicate_most_recent_wins` — Two CE visits with timestamps: older has Party=GREEN, newer has Party=LABOUR. Output should have Lab.
2. `test_ce_duplicate_fills_gaps_from_older` — Most recent visit has Party but no visit_notes. Older visit has visit_notes. Output has both.
3. `test_ce_duplicate_clash_logged` — Both visits have different visit_notes. Most recent wins, clash logged.
4. `test_ce_duplicate_no_timestamp_fallback` — No visit_visited_at column: falls back to file order (last wins). Regression test.
5. `test_ce_new_columns_attempt_result_status` — visit_attempt_status and visit_result_status appear in output.
6. `test_ce_1_5_maps_to_future_gvi` — `1-5` value from CE maps to future election GVI column.
7. `test_ce_comments_merged` — Comments from CE appears in output.

## Verification

1. `python3 -m pytest tools/test_enrichment.py -v --tb=short`
2. `python3 -m pytest tools/test_conversion.py -q` (regression)
3. Check QA report includes CE merge info when duplicates with timestamps exist
