#!/usr/bin/env python3
"""Test suite for validate_enrichment.py post-enrichment validation.

Usage:
    python3 tools/test_validation.py                            # All tests
    python3 tools/test_validation.py -v                         # Verbose
    python3 tools/test_validation.py TestFAILChecks             # Single class

Uses stdlib unittest. Zero external dependencies.
"""

import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
TOOL = SCRIPT_DIR / "validate_enrichment.py"
ENRICH_TOOL = SCRIPT_DIR / "enrich_register.py"
TEST_DATA = SCRIPT_DIR / "test_data"

BASE_CSV = TEST_DATA / "enrich_base.csv"
REGISTER_CSV = TEST_DATA / "enrich_register.csv"
EXPECTED_CSV = TEST_DATA / "enrich_expected.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_HEADERS = [
    "Elector No. Prefix", "Elector No.", "Elector No. Suffix",
    "Full Elector No.", "Surname", "Forename", "Middle Names",
    "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
    "PostCode", "UPRN",
]

ENRICHED_HEADERS = BASE_HEADERS + [
    "GE2024 Green Voting Intention", "GE2024 Party", "GE2024 Voted",
    "2026 Green Voting Intention", "2026 Party", "2026 Postal Voter",
]

SAMPLE_BASE_ROWS = [
    {
        "Elector No. Prefix": "KA1", "Elector No.": "1",
        "Elector No. Suffix": "0", "Full Elector No.": "KA1-1-0",
        "Surname": "Johnson", "Forename": "Emily", "Middle Names": "Rose",
        "Address1": "Flat 1", "Address2": "22 Willesden Lane",
        "Address3": "", "Address4": "", "Address5": "", "Address6": "",
        "PostCode": "NW10 4QB", "UPRN": "100001",
    },
    {
        "Elector No. Prefix": "KA1", "Elector No.": "2",
        "Elector No. Suffix": "0", "Full Elector No.": "KA1-2-0",
        "Surname": "Smith", "Forename": "John", "Middle Names": "",
        "Address1": "33 Willesden Lane", "Address2": "",
        "Address3": "", "Address4": "", "Address5": "", "Address6": "",
        "PostCode": "NW10 4QB", "UPRN": "100002",
    },
    {
        "Elector No. Prefix": "KA2", "Elector No.": "1",
        "Elector No. Suffix": "0", "Full Elector No.": "KA2-1-0",
        "Surname": "Patel", "Forename": "Arun", "Middle Names": "Kumar",
        "Address1": "7 Mapesbury Road", "Address2": "",
        "Address3": "", "Address4": "", "Address5": "", "Address6": "",
        "PostCode": "NW2 4HT", "UPRN": "100006",
    },
]


def make_enriched_rows(base_rows, election_data=None):
    """Create enriched rows from base rows, adding election columns."""
    enriched = []
    for i, row in enumerate(base_rows):
        r = dict(row)
        for h in ENRICHED_HEADERS:
            if h not in r:
                r[h] = ""
        if election_data and i < len(election_data):
            r.update(election_data[i])
        enriched.append(r)
    return enriched


def write_temp_csv(rows, headers, encoding="utf-8-sig", line_ending="\r\n"):
    """Write rows to a temp CSV and return the path."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers,
                                lineterminator=line_ending,
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def run_validate(output_file, extra_args=None):
    """Run validate_enrichment.py as a subprocess.

    Returns (returncode, stdout, stderr).
    """
    cmd = [sys.executable, str(TOOL), str(output_file)]
    if extra_args:
        cmd += extra_args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def read_report(text):
    """Parse machine-readable lines from report text."""
    machine_lines = []
    in_section = False
    for line in text.splitlines():
        if line.strip() == "### MACHINE-READABLE SECTION ###":
            in_section = True
            continue
        if line.strip() == "### END MACHINE-READABLE SECTION ###":
            break
        if in_section and line.strip():
            machine_lines.append(line.strip())
    return machine_lines


# ---------------------------------------------------------------------------
# TestFAILChecks
# ---------------------------------------------------------------------------

class TestFAILChecks(unittest.TestCase):
    """Tests for checks that should produce FAIL results."""

    def test_row_count_mismatch(self):
        """FAIL when output has different row count from base."""
        # Base has 3 rows, output has 2
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS[:2])
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("row_count", stdout)
        self.assertIn("FAIL", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_modified_base_column(self):
        """FAIL when protected columns (Surname, Address) are changed."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {"GE2024 Party": "Lab"},
            {},
        ])
        # Corrupt Surname in row 0
        output_rows[0]["Surname"] = "WRONG_NAME"
        # Corrupt Address1 in row 1
        output_rows[1]["Address1"] = "WRONG_ADDRESS"

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("base_column_integrity", stdout)
        self.assertIn("WRONG_NAME", stdout)
        self.assertIn("WRONG_ADDRESS", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_duplicate_elector_number(self):
        """FAIL when duplicate Full Elector No. values exist."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        # Make row 1 a duplicate of row 0
        output_rows[1]["Full Elector No."] = "KA1-1-0"

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        # Fix base row 1 to match the duplicate so integrity doesn't fail first
        base_mod = [dict(r) for r in SAMPLE_BASE_ROWS]
        base_mod[1]["Full Elector No."] = "KA1-1-0"
        base_path = write_temp_csv(base_mod, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("duplicate_elector_numbers", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_blank_elector_number(self):
        """FAIL when blank Full Elector No. values exist."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        output_rows[1]["Full Elector No."] = ""

        base_mod = [dict(r) for r in SAMPLE_BASE_ROWS]
        base_mod[1]["Full Elector No."] = ""
        base_path = write_temp_csv(base_mod, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("duplicate_elector_numbers", stdout)
        self.assertIn("blank", stdout.lower())

        os.unlink(base_path)
        os.unlink(output_path)

    def test_missing_base_header(self):
        """FAIL when base headers are missing from output."""
        # Output is missing PostCode column
        reduced_headers = [h for h in ENRICHED_HEADERS if h != "PostCode"]
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        for row in output_rows:
            row.pop("PostCode", None)

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, reduced_headers)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("base_headers_present", stdout)
        self.assertIn("PostCode", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_row_order_divergence(self):
        """FAIL when row 0 Full Elector No. differs between base and output."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        # Reverse the output rows
        output_rows = list(reversed(output_rows))

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("row_order", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_missing_bom(self):
        """FAIL when output file has no UTF-8 BOM."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {"GE2024 Party": "Lab"},
            {},
        ])

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        # Write without BOM
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS,
                                     encoding="utf-8")

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("file_format", stdout)
        self.assertIn("BOM", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_lf_only_line_endings(self):
        """FAIL when output has LF-only line endings (with BOM present)."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {"GE2024 Party": "Lab"},
            {},
        ])

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        # Write with BOM but LF-only line endings
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS,
                                     line_ending="\n")

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("file_format", stdout)
        self.assertIn("CRLF", stdout)

        os.unlink(base_path)
        os.unlink(output_path)


# ---------------------------------------------------------------------------
# TestWARNChecks
# ---------------------------------------------------------------------------

class TestWARNChecks(unittest.TestCase):
    """Tests for checks that should produce WARN results."""

    def test_low_match_rate(self):
        """WARN when match rate is below threshold (data estimate)."""
        # All election columns empty = 0% match rate
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
            "--min-match-rate", "0.5",
        ])
        # Should warn about match rate AND enrichment_had_effect
        self.assertIn("match_rate", stdout)
        self.assertIn("WARN", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_unknown_party_code(self):
        """WARN when Party value is not in VALID_PARTY_CODES."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "INVALID_PARTY", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {},
            {},
        ])

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertIn("party_codes", stdout)
        self.assertIn("WARN", stdout)
        self.assertIn("INVALID_PARTY", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_valid_L_code_no_warn(self):
        """No WARN for 'L' party code (valid alternate Labour code)."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "L", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "3"},
            {"GE2024 Party": "Lab", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "3"},
            {},
        ])

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        # party_codes should PASS, not WARN
        lines = read_report(stdout)
        party_lines = [l for l in lines if "party_codes" in l]
        for line in party_lines:
            self.assertIn("Level=PASS", line)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_invalid_gvi(self):
        """WARN when GVI value is outside 1-5 range."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Green Voting Intention": "9",
             "GE2024 Party": "G", "GE2024 Voted": "Y"},
            {},
            {},
        ])

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertIn("gvi_range", stdout)
        self.assertIn("WARN", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_voted_without_party(self):
        """WARN when Voted='v' but Party is blank."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Voted": "Y", "GE2024 Party": "",
             "GE2024 Green Voting Intention": ""},
            {},
            {},
        ])

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertIn("voted_party_consistency", stdout)
        self.assertIn("WARN", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_enrichment_no_effect(self):
        """WARN when all election columns are empty."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertIn("enrichment_had_effect", stdout)
        self.assertIn("WARN", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_elector_no_inconsistent(self):
        """WARN when Full Elector No. doesn't match components."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {},
            {},
        ])
        # Corrupt Full Elector No. to not match Prefix-No-Suffix
        output_rows[0]["Full Elector No."] = "WRONG-FORMAT"

        # Also fix the base so integrity doesn't fail
        base_mod = [dict(r) for r in SAMPLE_BASE_ROWS]
        base_mod[0]["Full Elector No."] = "WRONG-FORMAT"
        base_path = write_temp_csv(base_mod, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertIn("elector_no_consistency", stdout)
        self.assertIn("WARN", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_identical_headers_warns(self):
        """WARN when base and output have identical headers."""
        output_rows = [dict(r) for r in SAMPLE_BASE_ROWS]

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, BASE_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path,
        ])
        self.assertIn("identical_headers", stdout)
        self.assertIn("WARN", stdout)

        os.unlink(base_path)
        os.unlink(output_path)


# ---------------------------------------------------------------------------
# TestPASSScenarios
# ---------------------------------------------------------------------------

class TestPASSScenarios(unittest.TestCase):
    """Tests for scenarios that should PASS."""

    def test_golden_enrichment_passes(self):
        """Golden enrichment output passes all checks with no FAILs or WARNs."""
        rc, stdout, _ = run_validate(str(EXPECTED_CSV), [
            "--base", str(BASE_CSV),
            "--elections", "GE2024", "2026",
        ])

        # Check report summary
        self.assertIn("FAILED=0", stdout)
        # Should have at least some PASSED checks
        self.assertRegex(stdout, r"PASSED=\d+")
        # Exit code should be 0
        self.assertEqual(rc, 0)

    def test_empty_output_no_crash(self):
        """Output with headers but zero data rows doesn't crash."""
        base_path = write_temp_csv([], BASE_HEADERS)
        output_path = write_temp_csv([], ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        # Should produce a report without crashing
        self.assertIn("SUMMARY", stdout)
        self.assertIn("0 rows", stdout)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_no_enrichment_warns_not_fails(self):
        """Output with empty election columns gets WARNs, not FAILs."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        # Should NOT fail
        self.assertEqual(rc, 0)
        # But should have warnings
        self.assertIn("WARN", stdout)
        self.assertIn("FAILED=0", stdout)

        os.unlink(base_path)
        os.unlink(output_path)


# ---------------------------------------------------------------------------
# TestStrictMode
# ---------------------------------------------------------------------------

class TestStrictMode(unittest.TestCase):
    """Tests for --strict flag."""

    def test_strict_promotes_warns_to_exit_1(self):
        """--strict makes exit code 1 when there are WARNs."""
        # Empty election data = WARNs for enrichment_had_effect, match_rate
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)

        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        # Without strict: should pass (exit 0)
        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 0)

        # With strict: should fail (exit 1)
        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
            "--strict",
        ])
        self.assertEqual(rc, 1)
        self.assertIn("strict", stdout.lower())

        os.unlink(base_path)
        os.unlink(output_path)


# ---------------------------------------------------------------------------
# TestReportParsing
# ---------------------------------------------------------------------------

class TestReportParsing(unittest.TestCase):
    """Tests for enrichment report parsing."""

    def _write_report(self, machine_lines):
        """Write a minimal report file with machine-readable section."""
        fd, path = tempfile.mkstemp(suffix=".txt")
        os.close(fd)
        lines = [
            "Some human readable text",
            "",
            "### MACHINE-READABLE SECTION ###",
        ]
        lines.extend(machine_lines)
        lines.append("### END MACHINE-READABLE SECTION ###")
        Path(path).write_text("\n".join(lines), encoding="utf-8")
        return path

    def test_report_match_rate_used(self):
        """Match rate from report is preferred over data estimate."""
        # Report says 8/10 matched = 80%
        report_path = self._write_report([
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=A|BaseName=A|PostCode=NW1 1AA|Score=0.950",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=B|BaseName=B|PostCode=NW1 1AB|Score=0.900",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=C|BaseName=C|PostCode=NW1 1AC|Score=0.920",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=D|BaseName=D|PostCode=NW1 1AD|Score=0.880",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=E|BaseName=E|PostCode=NW1 1AE|Score=0.910",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=F|BaseName=F|PostCode=NW1 1AF|Score=0.890",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=G|BaseName=G|PostCode=NW1 1AG|Score=0.870",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=H|BaseName=H|PostCode=NW1 1AH|Score=0.860",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AI|Name=I Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AJ|Name=J Person",
        ])

        # Data has all empty elections = 0% from data, but report says 80%
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path,
            "--report", report_path,
            "--elections", "GE2024", "2026",
            "--min-match-rate", "0.7",
        ])
        # Match rate should come from report (80%) and pass threshold
        self.assertIn("report", stdout.lower())
        self.assertIn("80", stdout)

        os.unlink(base_path)
        os.unlink(output_path)
        os.unlink(report_path)

    def test_matched_but_empty_detected(self):
        """Rows matched per report but with empty election data produce WARN."""
        # Report says Emily Johnson at NW10 4QB was matched
        report_path = self._write_report([
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=Emily Johnson|BaseName=Emily Johnson"
            "|PostCode=NW10 4QB|Score=0.950",
        ])

        # But output has all election columns empty
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path,
            "--report", report_path,
            "--elections", "GE2024", "2026",
        ])
        self.assertIn("matched_but_empty", stdout)
        self.assertIn("WARN", stdout)

        os.unlink(base_path)
        os.unlink(output_path)
        os.unlink(report_path)

    def test_report_match_rate_below_threshold(self):
        """WARN when report-based match rate is below threshold."""
        # Report says 2/10 matched = 20%
        report_path = self._write_report([
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=A|BaseName=A|PostCode=NW1 1AA|Score=0.950",
            "MATCH|Source=enriched_register|Status=confident"
            "|ERName=B|BaseName=B|PostCode=NW1 1AB|Score=0.900",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AC|Name=C Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AD|Name=D Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AE|Name=E Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AF|Name=F Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AG|Name=G Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AH|Name=H Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AI|Name=I Person",
            "MATCH|Source=enriched_register|Status=unmatched"
            "|PostCode=NW1 1AJ|Name=J Person",
        ])

        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {},
            {},
        ])
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path,
            "--report", report_path,
            "--elections", "GE2024", "2026",
            "--min-match-rate", "0.7",
        ])
        # Match rate from report (20%) should fail threshold
        self.assertIn("match_rate", stdout)
        self.assertIn("WARN", stdout)
        self.assertIn("report", stdout.lower())
        lines = read_report(stdout)
        match_lines = [l for l in lines if "match_rate" in l]
        self.assertTrue(any("Level=WARN" in l for l in match_lines))

        os.unlink(base_path)
        os.unlink(output_path)
        os.unlink(report_path)

    def test_malformed_report_skipped(self):
        """Malformed report file doesn't crash — gracefully skipped."""
        fd, report_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd)
        Path(report_path).write_text("This is not a valid report\n",
                                     encoding="utf-8")

        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {"GE2024 Party": "Lab", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "3"},
            {},
        ])
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path,
            "--report", report_path,
            "--elections", "GE2024", "2026",
        ])
        # Should not crash
        self.assertIn("SUMMARY", stdout)

        os.unlink(base_path)
        os.unlink(output_path)
        os.unlink(report_path)


# ---------------------------------------------------------------------------
# TestCLI
# ---------------------------------------------------------------------------

class TestCLI(unittest.TestCase):
    """Tests for CLI argument handling."""

    def test_base_required(self):
        """Error when --base is not provided."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, stderr = run_validate(output_path)
        self.assertNotEqual(rc, 0)
        self.assertIn("--base", stderr)

        os.unlink(output_path)

    def test_quiet_suppresses_stdout(self):
        """--quiet suppresses stdout output."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {"GE2024 Party": "Lab", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "3"},
            {},
        ])
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path,
            "--elections", "GE2024", "2026",
            "--quiet",
        ])
        self.assertEqual(stdout.strip(), "")

        os.unlink(base_path)
        os.unlink(output_path)

    def test_exit_code_0_on_pass(self):
        """Exit code 0 when validation passes."""
        rc, stdout, _ = run_validate(str(EXPECTED_CSV), [
            "--base", str(BASE_CSV),
            "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 0)

    def test_exit_code_1_on_fail(self):
        """Exit code 1 when FAIL checks are triggered."""
        # Fewer rows than base = row count mismatch
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS[:1])
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        rc, _, _ = run_validate(output_path, [
            "--base", base_path, "--elections", "GE2024", "2026",
        ])
        self.assertEqual(rc, 1)

        os.unlink(base_path)
        os.unlink(output_path)

    def test_elections_override(self):
        """--elections overrides auto-discovery."""
        output_rows = make_enriched_rows(SAMPLE_BASE_ROWS, [
            {"GE2024 Party": "G", "GE2024 Voted": "Y",
             "GE2024 Green Voting Intention": "1"},
            {},
            {},
        ])
        base_path = write_temp_csv(SAMPLE_BASE_ROWS, BASE_HEADERS)
        output_path = write_temp_csv(output_rows, ENRICHED_HEADERS)

        # Only specify GE2024 — 2026 columns should not be checked
        rc, stdout, _ = run_validate(output_path, [
            "--base", base_path,
            "--elections", "GE2024",
        ])
        # Report should mention GE2024 coverage but not 2026
        self.assertIn("GE2024", stdout)

        os.unlink(base_path)
        os.unlink(output_path)


# ---------------------------------------------------------------------------
# TestEndToEnd
# ---------------------------------------------------------------------------

class TestEndToEnd(unittest.TestCase):
    """End-to-end tests running enrichment then validation."""

    def test_enrich_then_validate_passes(self):
        """Full pipeline: enrich_register.py then validate_enrichment.py."""
        tmpdir = tempfile.mkdtemp()
        output_path = os.path.join(tmpdir, "enriched.csv")
        report_path = os.path.join(tmpdir, "enriched.report.txt")

        # Step 1: Run enrichment
        enrich_cmd = [
            sys.executable, str(ENRICH_TOOL),
            str(BASE_CSV), output_path,
            "--enriched-register", str(REGISTER_CSV),
            "--historic-elections", "GE2024",
            "--future-elections", "2026",
            "--report", report_path,
            "--quiet",
        ]
        enrich_result = subprocess.run(enrich_cmd, capture_output=True,
                                       text=True)
        self.assertEqual(enrich_result.returncode, 0,
                         f"Enrichment failed: {enrich_result.stderr}")

        # Step 2: Run validation
        rc, stdout, stderr = run_validate(output_path, [
            "--base", str(BASE_CSV),
            "--report", report_path,
            "--elections", "GE2024", "2026",
        ])

        # Should pass with no FAILs
        self.assertIn("FAILED=0", stdout,
                       f"Validation had FAILs:\n{stdout}")
        self.assertEqual(rc, 0,
                         f"Exit code was {rc}, expected 0:\n{stdout}")


if __name__ == "__main__":
    unittest.main()
