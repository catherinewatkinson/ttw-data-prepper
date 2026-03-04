# Review QA Report

Walk a human through each change flagged in a QA report from `clean_register.py`, explaining what was done and why, and highlighting items that need manual review.

**Argument**: Path to the QA report file (e.g. `path/to/report.txt`)

## Instructions

1. Read the QA report file provided as argument: `$ARGUMENTS`
2. Parse the machine-readable section (between `### MACHINE-READABLE SECTION ###` markers) to extract all `DELETED`, `FIX`, and `WARNING` entries
3. Walk through changes in groups, presenting each group with a count header:

### a. Deletions (DELETED entries)

- Show each deleted record (elector ID, name, reason)
- Explain: "These records were deleted because they have no address data. UG C3 mandates that records without an address cannot be uploaded to TTW Digital."
- Ask: "Do these deletions look correct?"

### b. Auto-fixes (FIX entries), grouped by issue type

**Name normalization**: Show old -> new for each name fix. Explain: "Names were normalized to title case. Council data is sometimes exported in ALL CAPS." Highlight Mac/Mc names specifically -- these use heuristic rules and may occasionally misfire on non-Scottish names.

**Address gap fixes**: Show old -> new address fields. Explain: "Address data was shifted up to remove gaps (e.g., road name in Address3 when Address2 was empty)."

**Flat comma splits**: Show the split. Explain: "UG C3 recommends splitting 'Flat X, N Road' into Address1='Flat X' and Address2='N Road'."

**Number reorders**: Show old -> new. Explain: "UG C3 says the building name should come before the number, not after. '14 South House' was reordered to 'South House 14'."

For each group of fixes: show old value -> new value, ask "Does this look correct?"

### c. Manual review items (WARNING entries containing "NEEDS MANUAL FIX")

- Show each flagged record with its address data
- Explain why it couldn't be auto-fixed:
  - **Ampersand**: too complex to auto-parse (see UG C3 slide 12)
  - **Comma pattern**: ambiguous, not a standard 'Flat X, N Road' pattern
  - **Bracket notation**: unclear whether it's a range or specific address
- Ask: "What should the correct address be for this record?" and note the response for manual editing
- Offer to open the output CSV at the specific row for editing

### d. Other warnings (WARNING entries NOT containing "NEEDS MANUAL FIX")

- Group by type: date issues, missing postcodes, missing names, duplicates, postcode format
- Present counts and samples
- For duplicates: show the elector numbers and rows involved

## Summary

4. At the end, present a summary: N deletions, N auto-fixes, N items needing manual review, N other warnings
5. Offer to open the output CSV and the original input side-by-side for any record the human wants to inspect
6. Remind the user they can re-run the conversion after making manual fixes
