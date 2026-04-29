#!/usr/bin/env python3
"""Test suite for clean_register.py electoral register conversion.

Usage:
    python3 tools/test_conversion.py                                    # All tests
    python3 tools/test_conversion.py -v                                 # Verbose
    python3 tools/test_conversion.py TestGoldenFileRegisterOnly         # Single class
    python3 tools/test_conversion.py --verify INPUT OUTPUT [REPORT]     # User verification

Uses stdlib unittest. Zero external dependencies.
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
TOOL = SCRIPT_DIR / "clean_register.py"
TEST_DATA = SCRIPT_DIR / "test_data"

# Direct imports for unit-level tests (avoids subprocess overhead)
sys.path.insert(0, str(SCRIPT_DIR))
from clean_register import _norm_col, resolve_aliases, COLUMN_ALIASES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_clean(input_file, output_file, extra_args=None, report_file=None):
    """Run clean_register.py as a subprocess. Returns (returncode, stdout, stderr)."""
    cmd = [sys.executable, str(TOOL), str(input_file), str(output_file), "--quiet"]
    if report_file:
        cmd += ["--report", str(report_file)]
    if extra_args:
        cmd += extra_args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def read_output_csv(path):
    """Read a TTW-format output CSV and return (headers, rows)."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = list(reader)
    return headers, rows


def read_report(path):
    """Read QA report and return (full_text, machine_readable_lines)."""
    text = Path(path).read_text(encoding="utf-8")
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
    return text, machine_lines


def parse_machine_line(line):
    """Parse a machine-readable report line into (type, dict)."""
    parts = line.split("|")
    entry_type = parts[0]
    fields = {}
    for part in parts[1:]:
        key, _, value = part.partition("=")
        fields[key] = value
    return entry_type, fields


# ---------------------------------------------------------------------------
# Golden File Tests
# ---------------------------------------------------------------------------

class TestGoldenFileRegisterOnly(unittest.TestCase):
    """Test that golden input produces output matching the TTW test data exactly."""

    @classmethod
    def setUpClass(cls):
        cls.golden_input = TEST_DATA / "golden_input_register_only.csv"
        cls.golden_expected = TEST_DATA / "golden_expected_register_only.csv"
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, cls.stdout, cls.stderr = run_clean(
            cls.golden_input, cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"clean_register.py failed:\n{self.stderr}")

    def test_header_match(self):
        exp_h, _ = read_output_csv(self.golden_expected)
        got_h, _ = read_output_csv(self.tmp_output.name)
        self.assertEqual(exp_h, got_h,
            f"Column order mismatch.\nExpected: {exp_h}\nGot:      {got_h}")

    def test_row_count(self):
        _, exp_r = read_output_csv(self.golden_expected)
        _, got_r = read_output_csv(self.tmp_output.name)
        self.assertEqual(len(exp_r), len(got_r))

    def test_field_level_match(self):
        exp_h, exp_r = read_output_csv(self.golden_expected)
        _, got_r = read_output_csv(self.tmp_output.name)
        for i, (exp, got) in enumerate(zip(exp_r, got_r)):
            for col in exp_h:
                self.assertEqual(
                    exp.get(col, ""), got.get(col, ""),
                    f"Row {i+1}, column '{col}': expected {repr(exp.get(col, ''))}, "
                    f"got {repr(got.get(col, ''))}"
                )

    def test_bom_present(self):
        with open(self.tmp_output.name, "rb") as f:
            data = f.read()
        self.assertTrue(data.startswith(b"\xef\xbb\xbf"), "Output must start with UTF-8 BOM")

    def test_crlf_line_endings(self):
        with open(self.tmp_output.name, "rb") as f:
            data = f.read()
        segments = data.split(b"\n")
        for seg in segments[:-1]:
            self.assertTrue(seg.endswith(b"\r"),
                f"Expected CRLF line endings, found LF-only")

    def test_no_deletions(self):
        _, machine = read_report(self.tmp_report.name)
        deletions = [l for l in machine if l.startswith("DELETED")]
        self.assertEqual(len(deletions), 0, "Golden data should have zero deletions")


class TestGoldenFileRegisterPlusElections(unittest.TestCase):
    """Test that golden input with elections matches TTW test data exactly."""

    @classmethod
    def setUpClass(cls):
        cls.golden_input = TEST_DATA / "golden_input_register_plus_elections.csv"
        cls.golden_expected = TEST_DATA / "golden_expected_register_plus_elections.csv"
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, cls.stdout, cls.stderr = run_clean(
            cls.golden_input, cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "2022", "2026",
                "--election-types", "historic", "future",
            ],
            report_file=cls.tmp_report.name,
        )

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"clean_register.py failed:\n{self.stderr}")

    def test_header_match(self):
        exp_h, _ = read_output_csv(self.golden_expected)
        got_h, _ = read_output_csv(self.tmp_output.name)
        self.assertEqual(exp_h, got_h,
            f"Column order mismatch.\nExpected: {exp_h}\nGot:      {got_h}")

    def test_field_level_match(self):
        exp_h, exp_r = read_output_csv(self.golden_expected)
        _, got_r = read_output_csv(self.tmp_output.name)
        for i, (exp, got) in enumerate(zip(exp_r, got_r)):
            for col in exp_h:
                self.assertEqual(
                    exp.get(col, ""), got.get(col, ""),
                    f"Row {i+1}, column '{col}': expected {repr(exp.get(col, ''))}, "
                    f"got {repr(got.get(col, ''))}"
                )

    def test_election_columns_present(self):
        got_h, _ = read_output_csv(self.tmp_output.name)
        for col in ["2022 Green Voting Intention", "2022 Party", "2022 Voted",
                     "2026 Green Voting Intention", "2026 Party", "2026 Postal Voter"]:
            self.assertIn(col, got_h, f"Missing election column: {col}")


# ---------------------------------------------------------------------------
# Deletion Tests
# ---------------------------------------------------------------------------

class TestDeletion(unittest.TestCase):
    """Test that ONLY no-address records are deleted, everything else is kept."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"Failed:\n{self.stderr}")

    def test_exactly_one_deletion(self):
        deletions = [l for l in self.machine if l.startswith("DELETED")]
        self.assertEqual(len(deletions), 1,
            f"Expected exactly 1 deletion, got {len(deletions)}: {deletions}")

    def test_no_address_deleted(self):
        """Row 1 (Alice NoAddress) with all-empty address should be deleted."""
        deletions = [parse_machine_line(l) for l in self.machine if l.startswith("DELETED")]
        self.assertEqual(len(deletions), 1)
        _, fields = deletions[0]
        self.assertEqual(fields["RollNo"], "1")
        self.assertEqual(fields["Reason"], "no address")

    def test_deleted_not_in_output(self):
        """The deleted record should not appear in output."""
        surnames = [r["Surname"] for r in self.rows]
        self.assertNotIn("NoAddress", surnames)

    def test_postcode_only_kept(self):
        """Row 2 (Bob PostcodeOnly) should be flagged but kept."""
        surnames = [r["Surname"] for r in self.rows]
        self.assertIn("PostcodeOnly", surnames)

    def test_postcode_only_flagged(self):
        """Postcode-only record should have a WARNING."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        address_warns = [w for _, w in warnings if "PostCode present but no street" in w.get("Issue", "")]
        self.assertTrue(len(address_warns) >= 1, "Postcode-only should generate a warning")

    def test_partial_address_kept(self):
        """Row 3 (Carol PartialAddr) with address but no postcode should be kept."""
        surnames = [r["Surname"] for r in self.rows]
        self.assertIn("PartialAddr", surnames)

    def test_output_row_count(self):
        """84 input rows - 1 deletion = 83 output rows."""
        self.assertEqual(len(self.rows), 83)

    def test_deletion_reason_in_report(self):
        """Deleted record should appear in human-readable section too."""
        self.assertIn("no address", self.report_text)
        self.assertIn("NoAddress", self.report_text)


# ---------------------------------------------------------------------------
# Field Mapping Tests
# ---------------------------------------------------------------------------

class TestFieldMapping(unittest.TestCase):
    """Test council→TTW field mapping."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, _ = run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, _ = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_council_columns_preserved_in_output(self):
        """Council-only columns should be preserved in output by default."""
        # FIELD_MAP source columns (e.g. RegisteredAddress1) are consumed by mapping
        # but non-mapped council columns should now pass through
        preserved = {"ElectorTitle", "IERStatus", "FranchiseMarker",
                     "Euro", "Parl", "County", "Ward",
                     "MethodOfVerification", "ElectorID",
                     "SubHouse", "House"}
        for col in preserved:
            self.assertIn(col, self.headers, f"Council column '{col}' should be preserved")
        # FIELD_MAP source columns should NOT be in output (mapped to TTW names)
        self.assertNotIn("RegisteredAddress1", self.headers)

    def test_ttw_columns_present(self):
        """Core TTW columns should be in output."""
        required = ["Elector No. Prefix", "Elector No.", "Elector No. Suffix",
                     "Full Elector No.", "Surname", "Forename",
                     "Address1", "Address2", "PostCode"]
        for col in required:
            self.assertIn(col, self.headers, f"Missing TTW column: {col}")

    def test_empty_columns_preserved(self):
        """Empty optional columns like UPRN should still be in output."""
        self.assertIn("UPRN", self.headers, "UPRN column should be preserved even if empty")
        self.assertIn("Address5", self.headers, "Address5 should be preserved even if empty")
        self.assertIn("Address6", self.headers, "Address6 should be preserved even if empty")

    def test_postcode_normalized(self):
        """PostCode should be uppercase with normalized spacing."""
        # Row 13: input "  NR5   9LD  " → output "NR5 9LD"
        spacey = [r for r in self.rows if r["Surname"] == "SpaceyPostcode"]
        self.assertEqual(len(spacey), 1)
        self.assertEqual(spacey[0]["PostCode"], "NR5 9LD")

    def test_uprn_passthrough(self):
        """UPRN should pass through unchanged."""
        self.assertIn("UPRN", self.headers)

    def test_non_ascii_preserved(self):
        """Non-ASCII characters should be preserved."""
        obrien = [r for r in self.rows if "O'Brien" in r.get("Surname", "")]
        self.assertTrue(len(obrien) >= 1, "O'Brien-Smythe should be in output")
        self.assertEqual(obrien[0]["Surname"], "O'Brien-Smythe")

    def test_subhouse_house_composed_into_address1(self):
        """SubHouse/House are composed into Address1/Address2; council's
        redundant duplicate of House in RegisteredAddress1 is collapsed out."""
        kate = [r for r in self.rows if r["Surname"] == "WithSubHouse"]
        self.assertEqual(len(kate), 1)
        # Fixture has SubHouse="Flat 3", House="Oak Manor",
        # RegisteredAddress1="Oak Manor" (council redundancy),
        # RegisteredAddress2="21 Willesden Lane".
        self.assertEqual(kate[0]["Address1"], "Flat 3")
        self.assertEqual(kate[0]["Address2"], "Oak Manor")
        # RA1 was a dup of House — dropped from the shift; RA2 lands at Address3.
        self.assertEqual(kate[0]["Address3"], "21 Willesden Lane")

    def test_no_discarded_columns_by_default(self):
        """Without --strip-extra, no columns should be listed as discarded."""
        self.assertNotIn("Discarded columns", self.report_text)


# ---------------------------------------------------------------------------
# Suffix Tests
# ---------------------------------------------------------------------------

class TestSuffix(unittest.TestCase):
    """Test auto suffix computation (decimal RollNo detection)."""

    def test_integer_rollnos_get_suffix_zero(self):
        """When all RollNos are integers and no Suffix column, every row gets suffix '0'."""
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", newline="")
        import csv as _csv
        writer = _csv.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname",
                         "ElectorForename", "RegisteredAddress1", "PostCode"])
        writer.writerow(["AA1", "1", "Smith", "John", "1 Road", "NW1 1AA"])
        writer.writerow(["AA1", "2", "Jones", "Jane", "2 Road", "NW1 1AA"])
        tmp_in.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(tmp_in.name, tmp_out.name)
        self.assertEqual(rc, 0, stderr)
        _, rows = read_output_csv(tmp_out.name)
        os.unlink(tmp_in.name)
        os.unlink(tmp_out.name)
        for i, r in enumerate(rows):
            self.assertEqual(r["Elector No. Suffix"], "0",
                f"Row {i+1}: suffix should be '0', got '{r['Elector No. Suffix']}'")

    def test_suffix_column_used_when_present(self):
        """When input has a Suffix column and integer RollNos, use Suffix values as-is."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
        )
        self.assertEqual(rc, 0, stderr)
        _, rows = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        # Golden data has Suffix column with non-zero values — should be preserved
        self.assertTrue(any(r["Elector No. Suffix"] != "0" for r in rows),
            "Suffix column values should be used when present")

    def test_full_elector_no_format(self):
        """Full Elector No. should be Prefix-No-Suffix."""
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", newline="")
        import csv as _csv
        writer = _csv.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname",
                         "ElectorForename", "RegisteredAddress1", "PostCode"])
        writer.writerow(["AA1", "1", "Smith", "John", "1 Road", "NW1 1AA"])
        tmp_in.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(tmp_in.name, tmp_out.name)
        self.assertEqual(rc, 0, stderr)
        _, rows = read_output_csv(tmp_out.name)
        os.unlink(tmp_in.name)
        os.unlink(tmp_out.name)
        for i, r in enumerate(rows):
            expected = f"{r['Elector No. Prefix']}-{r['Elector No.']}-{r['Elector No. Suffix']}"
            self.assertEqual(r["Full Elector No."], expected,
                f"Row {i+1}: expected '{expected}', got '{r['Full Elector No.']}'")

    def test_decimal_rollno_split(self):
        """Decimal RollNos should be split into integer Elector No. + sequential suffix."""
        # Create minimal CSV with decimal RollNos
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", newline="")
        import csv as _csv
        writer = _csv.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname",
                         "ElectorForename", "RegisteredAddress1", "PostCode"])
        writer.writerow(["AA1", "10", "Base", "A", "1 Road", "NW1 1AA"])
        writer.writerow(["AA1", "10.5", "Half", "B", "1 Road", "NW1 1AA"])
        writer.writerow(["AA1", "10.75", "Three", "C", "1 Road", "NW1 1AA"])
        writer.writerow(["AA1", "20", "Solo", "D", "2 Road", "NW1 1AA"])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(tmp_in.name, tmp_out.name)
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        _, rows = read_output_csv(tmp_out.name)
        os.unlink(tmp_in.name)
        os.unlink(tmp_out.name)

        # RollNo 10 (frac=0) -> suffix 0, RollNo 10.5 (frac=0.5) -> suffix 1, RollNo 10.75 -> suffix 2
        base = [r for r in rows if r["Surname"] == "Base"][0]
        half = [r for r in rows if r["Surname"] == "Half"][0]
        three = [r for r in rows if r["Surname"] == "Three"][0]
        solo = [r for r in rows if r["Surname"] == "Solo"][0]

        self.assertEqual(base["Elector No."], "10")
        self.assertEqual(base["Elector No. Suffix"], "0")
        self.assertEqual(half["Elector No."], "10")
        self.assertEqual(half["Elector No. Suffix"], "1")
        self.assertEqual(three["Elector No."], "10")
        self.assertEqual(three["Elector No. Suffix"], "2")
        self.assertEqual(solo["Elector No."], "20")
        self.assertEqual(solo["Elector No. Suffix"], "0")

        # Full Elector No. format
        self.assertEqual(base["Full Elector No."], "AA1-10-0")
        self.assertEqual(half["Full Elector No."], "AA1-10-1")
        self.assertEqual(three["Full Elector No."], "AA1-10-2")
        self.assertEqual(solo["Full Elector No."], "AA1-20-0")

    def test_decimal_only_group(self):
        """Group with only decimal RollNos (no whole number) should still work."""
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", newline="")
        import csv as _csv
        writer = _csv.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname",
                         "ElectorForename", "RegisteredAddress1", "PostCode"])
        writer.writerow(["AA1", "5.5", "Alpha", "A", "1 Road", "NW1 1AA"])
        writer.writerow(["AA1", "5.75", "Beta", "B", "1 Road", "NW1 1AA"])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(tmp_in.name, tmp_out.name)
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        _, rows = read_output_csv(tmp_out.name)
        os.unlink(tmp_in.name)
        os.unlink(tmp_out.name)

        alpha = [r for r in rows if r["Surname"] == "Alpha"][0]
        beta = [r for r in rows if r["Surname"] == "Beta"][0]
        self.assertEqual(alpha["Elector No."], "5")
        self.assertEqual(alpha["Elector No. Suffix"], "0")
        self.assertEqual(beta["Elector No."], "5")
        self.assertEqual(beta["Elector No. Suffix"], "1")


# ---------------------------------------------------------------------------
# Date of Attainment Tests
# ---------------------------------------------------------------------------

class TestDateOfAttainment(unittest.TestCase):
    """Test date normalization."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def _get_row(self, surname):
        matches = [r for r in self.rows if r.get("Surname") == surname]
        self.assertTrue(len(matches) >= 1, f"Row with Surname={surname} not found")
        return matches[0]

    def test_date_column_present_when_data_exists(self):
        """Date of Attainment column should be present because edge cases have dates."""
        self.assertIn("Date of Attainment", self.headers,
            "Date of Attainment should be in output when input has date data")

    def test_ddmmyyyy_passthrough(self):
        """DD/MM/YYYY input should pass through unchanged."""
        row = self._get_row("DateDMY")
        self.assertEqual(row["Date of Attainment"], "15/03/2008")

    def test_iso_converted(self):
        """YYYY-MM-DD input should convert to DD/MM/YYYY."""
        row = self._get_row("DateISO")
        self.assertEqual(row["Date of Attainment"], "15/03/2008")

    def test_empty_stays_empty(self):
        """Empty date should remain empty."""
        row = self._get_row("DateEmpty")
        self.assertEqual(row["Date of Attainment"], "")

    def test_invalid_blanked_and_warned(self):
        """Invalid date should be blanked with a WARNING."""
        row = self._get_row("DateInvalid")
        self.assertEqual(row["Date of Attainment"], "")
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        date_warns = [w for _, w in warnings if "unparseable" in w.get("Issue", "")]
        self.assertTrue(len(date_warns) >= 1, "Invalid date should generate WARNING")

    def test_unreasonable_year_blanked(self):
        """Year 1802 should be blanked with a WARNING."""
        row = self._get_row("DateOldYear")
        self.assertEqual(row["Date of Attainment"], "")
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        year_warns = [w for _, w in warnings if "unreasonable year" in w.get("Issue", "")]
        self.assertTrue(len(year_warns) >= 1, "Unreasonable year should generate WARNING")

    def test_column_absent_when_all_empty(self):
        """When all dates are empty (golden data), Date of Attainment should not be in output."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, _ = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=[],
        )
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(rc, 0)
        self.assertNotIn("Date of Attainment", headers,
            "Date of Attainment should not appear when all dates are empty")


# ---------------------------------------------------------------------------
# Column Order Tests
# ---------------------------------------------------------------------------

class TestColumnOrder(unittest.TestCase):
    """Test that output column order matches TTW test data format."""

    def test_register_only_column_order(self):
        """Register-only output should have TTW columns first, then extras in input order."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, _ = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=[],
        )
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(rc, 0)

        ttw_core = [
            "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
            "Surname", "Forename", "Middle Names",
            "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
            "PostCode", "UPRN",
        ]
        # TTW core columns should come first in the correct order
        self.assertEqual(headers[:len(ttw_core)], ttw_core,
            f"TTW columns not in expected order.\nExpected: {ttw_core}\nGot:      {headers[:len(ttw_core)]}")
        # Extra columns should follow
        extras = headers[len(ttw_core):]
        self.assertTrue(len(extras) > 0, "Extra input columns should be preserved")

    def test_register_plus_elections_column_order(self):
        """Register+elections output should have core + election columns first, then extras."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, _ = run_clean(
            TEST_DATA / "golden_input_register_plus_elections.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "2022", "2026",
                "--election-types", "historic", "future",
            ],
        )
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(rc, 0)

        ttw_core = [
            "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
            "Surname", "Forename", "Middle Names",
            "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
            "PostCode", "UPRN",
            "2022 Green Voting Intention", "2022 Party", "2022 Voted",
            "2026 Green Voting Intention", "2026 Party", "2026 Postal Voter",
        ]
        # TTW + election columns should come first in the correct order
        self.assertEqual(headers[:len(ttw_core)], ttw_core)
        # Extra columns should follow
        extras = headers[len(ttw_core):]
        self.assertTrue(len(extras) > 0, "Extra input columns should be preserved")


# ---------------------------------------------------------------------------
# Encoding Tests
# ---------------------------------------------------------------------------

class TestEncoding(unittest.TestCase):
    """Test output encoding (BOM, CRLF, non-ASCII)."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
        )

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)

    def test_bom_bytes(self):
        """Output should start with UTF-8 BOM bytes EF BB BF."""
        with open(self.tmp_output.name, "rb") as f:
            data = f.read()
        self.assertEqual(data[:3], b"\xef\xbb\xbf",
            f"Expected BOM, got first 3 bytes: {data[:3].hex()}")

    def test_crlf_line_endings(self):
        """All line endings should be CRLF."""
        with open(self.tmp_output.name, "rb") as f:
            data = f.read()
        segments = data.split(b"\n")
        for i, seg in enumerate(segments[:-1]):
            self.assertTrue(seg.endswith(b"\r"),
                f"Line {i+1}: expected CRLF, got LF-only")

    def test_non_ascii_preserved(self):
        """Non-ASCII characters should survive encoding."""
        _, rows = read_output_csv(self.tmp_output.name)
        obrien = [r for r in rows if "O'Brien" in r.get("Surname", "")]
        self.assertTrue(len(obrien) >= 1)

    def test_latin1_input_handled(self):
        """Latin-1 encoded input should be auto-detected and processed."""
        # Create a Latin-1 test file
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="wb")
        headers = "PDCode,RollNo,ElectorTitle,ElectorSurname,ElectorForename,ElectorMiddleName,RegisteredAddress1,RegisteredAddress2,PostCode,UPRN\r\n"
        row = 'EB1,1,Mr,M\xfcller,Hans,,10 High St,London,NW1 1AA,\r\n'
        tmp_in.write(headers.encode("latin-1"))
        tmp_in.write(row.encode("latin-1"))
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()

        rc, _, stderr = run_clean(tmp_in.name, tmp_out.name)
        self.assertEqual(rc, 0, f"Latin-1 input failed:\n{stderr}")

        _, rows = read_output_csv(tmp_out.name)
        self.assertTrue(len(rows) >= 1)

        os.unlink(tmp_in.name)
        os.unlink(tmp_out.name)


# ---------------------------------------------------------------------------
# Malformed Input Tests
# ---------------------------------------------------------------------------

class TestMalformedInput(unittest.TestCase):
    """Test error handling for broken CSV files."""

    def _run_expect_error(self, input_name):
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(TEST_DATA / input_name, tmp_out.name)
        os.unlink(tmp_out.name)
        return rc, stderr

    def test_missing_required_column(self):
        """Missing required column should give a clear error."""
        rc, stderr = self._run_expect_error("malformed_missing_header.csv")
        self.assertNotEqual(rc, 0)
        self.assertIn("Missing required columns", stderr)
        self.assertIn("RollNo", stderr)

    def test_empty_file(self):
        """Header-only file should give zero rows error."""
        rc, stderr = self._run_expect_error("malformed_empty.csv")
        self.assertNotEqual(rc, 0)
        self.assertIn("zero data rows", stderr)

    def test_ttw_format_detected(self):
        """TTW-format input should trigger file-swap detection."""
        rc, stderr = self._run_expect_error("malformed_ttw_format.csv")
        self.assertNotEqual(rc, 0)
        self.assertIn("TTW format", stderr)

    def test_extra_commas_handled(self):
        """Inconsistent field counts should be handled gracefully."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        # This may succeed with warnings or fail — either is acceptable
        rc, _, stderr = run_clean(TEST_DATA / "malformed_extra_commas.csv", tmp_out.name)
        os.unlink(tmp_out.name)
        # Just verify it doesn't crash with an unhandled exception
        self.assertNotIn("Traceback", stderr,
            "Should not produce an unhandled Python traceback")


# ---------------------------------------------------------------------------
# Election Data Tests
# ---------------------------------------------------------------------------

class TestElectionData(unittest.TestCase):
    """Test election column mapping and validation."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "2022", "2026",
                "--election-types", "historic", "future",
            ],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_historic_columns(self):
        """Historic election should have Voted, not Postal Voter."""
        self.assertIn("2022 Voted", self.headers)
        self.assertNotIn("2022 Postal Voter", self.headers)

    def test_future_columns(self):
        """Future election should have Postal Voter, not Voted."""
        self.assertIn("2026 Postal Voter", self.headers)
        self.assertNotIn("2026 Voted", self.headers)

    def test_voted_flag_normalized(self):
        """Non-blank voted → 'Y'."""
        vote_green = [r for r in self.rows if r["Surname"] == "VoteGreen"]
        self.assertEqual(len(vote_green), 1)
        self.assertEqual(vote_green[0]["2022 Voted"], "Y")

    def test_voted_any_value_becomes_Y(self):
        """Any non-blank value → 'Y'."""
        voted_any = [r for r in self.rows if r["Surname"] == "VotedAny"]
        self.assertEqual(len(voted_any), 1)
        self.assertEqual(voted_any[0]["2022 Voted"], "Y")
        self.assertEqual(voted_any[0]["2026 Postal Voter"], "Y")

    def test_blank_voted_stays_blank(self):
        """Blank voted should stay blank."""
        blank = [r for r in self.rows if r["Surname"] == "BlankElection"]
        self.assertEqual(len(blank), 1)
        self.assertEqual(blank[0]["2022 Voted"], "")
        self.assertEqual(blank[0]["2026 Postal Voter"], "")

    def test_voting_intention_validation(self):
        """Invalid voting intention should generate WARNING."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        vi_warns = [w for _, w in warnings if "voting intention" in w.get("Issue", "").lower()]
        self.assertTrue(len(vi_warns) >= 1,
            "Invalid voting intention (7, X) should generate warnings")

    def test_unknown_party_flagged(self):
        """Unknown party codes should generate WARNING but be kept."""
        unknown = [r for r in self.rows if r["Surname"] == "UnknownParty"]
        self.assertEqual(len(unknown), 1)
        # UKIP and SNP should be kept as-is
        self.assertEqual(unknown[0]["2022 Party"], "UKIP")
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        party_warns = [w for _, w in warnings if "party code" in w.get("Issue", "").lower()]
        self.assertTrue(len(party_warns) >= 1, "Unknown party codes should generate warnings")


# ---------------------------------------------------------------------------
# Duplicate Detection Tests
# ---------------------------------------------------------------------------

class TestDuplicateDetection(unittest.TestCase):
    """Test duplicate PDCode+RollNo detection."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, _ = run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_duplicates_flagged(self):
        """Duplicate decimal RollNo should be normalized to distinct suffixes."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        suffix_fixes = [f for _, f in fixes if "suffix normalized" in f.get("Issue", "")]
        self.assertTrue(len(suffix_fixes) >= 1,
            "Decimal duplicates should be resolved via suffix normalization")

    def test_duplicates_kept(self):
        """Both duplicate rows should be in output."""
        bracket_rows = [r for r in self.rows if r["Elector No."] == "19"]
        self.assertEqual(len(bracket_rows), 2,
            "Both rows with RollNo=19 should be in output")


# ---------------------------------------------------------------------------
# Row Count Warning Tests
# ---------------------------------------------------------------------------

class TestRowCountWarning(unittest.TestCase):
    """Test --max-rows warning."""

    def test_row_count_warning(self):
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()

        run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=["--max-rows", "5"],
            report_file=tmp_report.name,
        )
        report_text, _ = read_report(tmp_report.name)
        os.unlink(tmp_out.name)
        os.unlink(tmp_report.name)

        self.assertIn("exceeding", report_text.lower(),
            "Should warn when row count exceeds --max-rows")


# ---------------------------------------------------------------------------
# QA Report Tests
# ---------------------------------------------------------------------------

class TestQAReport(unittest.TestCase):
    """Test QA report generation."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_report_created(self):
        self.assertTrue(len(self.report_text) > 0)

    def test_report_has_summary(self):
        self.assertIn("Summary", self.report_text)
        self.assertIn("Total input rows", self.report_text)

    def test_report_has_deletions(self):
        self.assertIn("Deleted Records", self.report_text)

    def test_report_has_warnings(self):
        self.assertIn("Warnings", self.report_text)

    def test_machine_readable_section(self):
        self.assertIn("### MACHINE-READABLE SECTION ###", self.report_text)
        self.assertIn("### END MACHINE-READABLE SECTION ###", self.report_text)

    def test_machine_lines_parseable(self):
        """All machine-readable lines should be parseable."""
        for line in self.machine:
            entry_type, fields = parse_machine_line(line)
            self.assertIn(entry_type, ("DELETED", "WARNING", "FIX"),
                f"Unknown entry type: {entry_type}")
            if entry_type == "DELETED":
                self.assertIn("RollNo", fields)
                self.assertIn("Reason", fields)
            elif entry_type == "WARNING":
                self.assertIn("Row", fields)
                self.assertIn("Field", fields)
            elif entry_type == "FIX":
                self.assertIn("Row", fields)
                self.assertIn("Field", fields)
                self.assertIn("Old", fields)
                self.assertIn("New", fields)
                self.assertIn("Issue", fields)


# ---------------------------------------------------------------------------
# Name Normalization Tests
# ---------------------------------------------------------------------------

class TestNameNormalization(unittest.TestCase):
    """Test name casing normalization (ALL CAPS / all lowercase -> title case)."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def _get_row(self, rollno):
        matches = [r for r in self.rows if r["Elector No."] == rollno]
        self.assertTrue(len(matches) >= 1, f"Row with RollNo={rollno} not found")
        return matches[0]

    def test_all_caps_to_title(self):
        """ALL CAPS name -> title case (row 31: JOHN SMITH -> John Smith)."""
        row = self._get_row("31")
        self.assertEqual(row["Forename"], "John")
        self.assertEqual(row["Surname"], "Smith")

    def test_lowercase_to_title(self):
        """all lowercase -> title case (row 32: jane doe -> Jane Doe)."""
        row = self._get_row("32")
        self.assertEqual(row["Forename"], "Jane")
        self.assertEqual(row["Surname"], "Doe")

    def test_hyphenated_caps(self):
        """JEAN-CLAUDE -> Jean-Claude (row 33)."""
        row = self._get_row("33")
        self.assertEqual(row["Forename"], "Jean-Claude")

    def test_van_damme_caps(self):
        """VAN DAMME -> Van Damme (row 33)."""
        row = self._get_row("33")
        self.assertEqual(row["Surname"], "Van Damme")

    def test_apostrophe_caps(self):
        """O'BRIEN -> O'Brien (row 34)."""
        row = self._get_row("34")
        self.assertEqual(row["Surname"], "O'Brien")

    def test_mc_prefix(self):
        """MCDONALD -> McDonald (row 35)."""
        row = self._get_row("35")
        self.assertEqual(row["Surname"], "McDonald")

    def test_mac_prefix(self):
        """MACDONALD -> MacDonald (row 36)."""
        row = self._get_row("36")
        self.assertEqual(row["Surname"], "MacDonald")

    def test_mixed_case_unchanged(self):
        """Mixed case stays (row 37: Sarah McDonald)."""
        row = self._get_row("37")
        self.assertEqual(row["Forename"], "Sarah")
        self.assertEqual(row["Surname"], "McDonald")

    def test_empty_name_no_crash(self):
        """Row 15 has empty Forename — should not crash."""
        row = self._get_row("15")
        self.assertEqual(row["Forename"], "")

    def test_existing_obrien_smythe_unchanged(self):
        """O'Brien-Smythe (mixed case, row 10) should stay unchanged."""
        row = self._get_row("10")
        self.assertEqual(row["Surname"], "O'Brien-Smythe")

    def test_fixes_logged_in_report(self):
        """FIX entries should exist for name case normalization."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        name_fixes = [f for _, f in fixes if "name case normalized" in f.get("Issue", "")]
        self.assertTrue(len(name_fixes) >= 1, "Name normalization should produce FIX entries")

    def test_address_not_title_cased(self):
        """Address fields should NOT be title-cased. Row 31 has Addr2='LONDON' -> stays 'LONDON'."""
        row = self._get_row("31")
        self.assertEqual(row["Address2"], "LONDON")


# ---------------------------------------------------------------------------
# Address Reformatting Tests
# ---------------------------------------------------------------------------

class TestAddressReformatting(unittest.TestCase):
    """Test address auto-fix and flagging behavior."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "edge_cases.csv", cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def _get_row(self, rollno):
        matches = [r for r in self.rows if r["Elector No."] == rollno]
        self.assertTrue(len(matches) >= 1, f"Row with RollNo={rollno} not found")
        return matches[0]

    def test_address_gap_shifted(self):
        """Row 38: Addr1='Flat 5', Addr2='', Addr3='88 Kilburn Lane' -> Addr2='88 Kilburn Lane'."""
        row = self._get_row("38")
        self.assertEqual(row["Address1"], "Flat 5")
        self.assertEqual(row["Address2"], "88 Kilburn Lane")
        self.assertEqual(row["Address3"], "")

    def test_flat_comma_split_empty_addr2(self):
        """Row 39: 'Flat 7, 45 High Road' with empty Addr2 -> split."""
        row = self._get_row("39")
        self.assertEqual(row["Address1"], "Flat 7")
        self.assertEqual(row["Address2"], "45 High Road")

    def test_flat_comma_occupied_addr2_not_split(self):
        """Row 40: 'Flat 7, 45 High Road' with Addr2='London' -> NOT split, NOT warned."""
        row = self._get_row("40")
        self.assertEqual(row["Address1"], "Flat 7, 45 High Road")
        self.assertEqual(row["Address2"], "London")
        # Should NOT have a warning for this row
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        row40_warns = [w for _, w in warnings if w.get("Row") == "41"]  # row_num = index+2 = 40+2=42.. wait
        # Row 40 in data = row_num 41 in report (header + 0-indexed)
        # Actually row_num = index + 2, index = 39 (0-based), so row_num = 41
        # Let's just check that there's no flat-comma warning for "Flat 7, 45 High Road" with occupied addr2
        flat_comma_warns = [w for _, w in warnings
                           if w.get("Value", "") == "Flat 7, 45 High Road"
                           and "comma" in w.get("Issue", "").lower()]
        self.assertEqual(len(flat_comma_warns), 0,
            "Flat comma with occupied Address2 should NOT generate a warning")

    def test_number_before_flat_reordered(self):
        """Row 41: '56 Flat 1' with empty Addr2 -> Addr1='Flat 1', Addr2='56'."""
        row = self._get_row("41")
        self.assertEqual(row["Address1"], "Flat 1")
        self.assertEqual(row["Address2"], "56")

    def test_single_char_remainder_not_reordered(self):
        """Row 42: '14 B' stays '14 B' (single char, likely alphanumeric house number)."""
        row = self._get_row("42")
        self.assertEqual(row["Address1"], "14 B")

    def test_single_word_remainder_not_reordered(self):
        """Row 43: '14 London' stays '14 London' (single word, ambiguous)."""
        row = self._get_row("43")
        self.assertEqual(row["Address1"], "14 London")

    def test_comma_free_flat_road_split(self):
        """Row 44: 'Flat 3 30 Chamberlayne Road' with empty Addr2 -> split."""
        row = self._get_row("44")
        self.assertEqual(row["Address1"], "Flat 3")
        self.assertEqual(row["Address2"], "30 Chamberlayne Road")

    def test_comma_free_flat_road_no_number(self):
        """Row 45: 'Flat 3 Chamberlayne Road' with empty Addr2 -> split."""
        row = self._get_row("45")
        self.assertEqual(row["Address1"], "Flat 3")
        self.assertEqual(row["Address2"], "Chamberlayne Road")

    def test_flat_building_not_split(self):
        """Row 46: 'Flat 3 Ontario Point' (no road suffix) -> NOT split."""
        row = self._get_row("46")
        self.assertEqual(row["Address1"], "Flat 3 Ontario Point")
        self.assertEqual(row["Address2"], "")

    def test_comma_free_flat_occupied_addr2(self):
        """Row 47: 'Flat 3 30 Chamberlayne Road' with Addr2='London' -> NOT split, but WARNING."""
        row = self._get_row("47")
        self.assertEqual(row["Address1"], "Flat 3 30 Chamberlayne Road")
        self.assertEqual(row["Address2"], "London")
        # Should still get advisory warning (long flat-prefix, no comma)
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        row47_warns = [w for _, w in warnings if "may need manual splitting" in w.get("Issue", "")
                      and "Flat 3 30 Chamberlayne Road" in w.get("Value", "")]
        self.assertTrue(len(row47_warns) >= 1,
            "Long flat-prefix Address1 should generate advisory warning even when Addr2 is occupied")

    def test_alphanumeric_before_building(self):
        """Row 48: '14A South House' -> 'South House 14A' (Fix 4 with alphanumeric)."""
        row = self._get_row("48")
        self.assertEqual(row["Address1"], "South House 14A")
        self.assertEqual(row["Address2"], "Coleman Road")

    def test_alphanumeric_before_flat(self):
        """Row 49: '14A Flat 1' with empty Addr2 -> Addr1='Flat 1', Addr2='14A'."""
        row = self._get_row("49")
        self.assertEqual(row["Address1"], "Flat 1")
        self.assertEqual(row["Address2"], "14A")

    def test_long_flat_prefix_advisory_warning(self):
        """Row 50: Long flat designation should NOT split and should generate advisory WARNING."""
        row = self._get_row("50")
        # Should NOT be split (multi-word flat ID)
        self.assertEqual(row["Address1"], "Flat Ground Floor 30 Chamberlayne Road")
        self.assertEqual(row["Address2"], "")
        # Should have advisory warning
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        advisory_warns = [w for _, w in warnings if "may need manual splitting" in w.get("Issue", "")
                         and "Flat Ground Floor" in w.get("Value", "")]
        self.assertTrue(len(advisory_warns) >= 1,
            "Long flat-prefix Address1 should generate advisory warning")

    def test_ampersand_auto_replaced(self):
        """Ampersand in Address1 should be auto-replaced with 'and'."""
        row = self._get_row("17")
        self.assertEqual(row["Address1"], "1ST and 2ND")
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        amp_fixes = [f for _, f in fixes if "ampersand" in f.get("Issue", "").lower()]
        self.assertTrue(len(amp_fixes) >= 1)

    def test_non_flat_comma_flagged(self):
        """Non-flat comma in Address1 should be flagged as NEEDS MANUAL FIX."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        comma_warns = [w for _, w in warnings if "comma" in w.get("Issue", "").lower()
                       and "NEEDS MANUAL FIX" in w.get("Issue", "")]
        self.assertTrue(len(comma_warns) >= 1)

    def test_bracket_notation_no_warning(self):
        """Bracket notation is valid per UG C3 slide 10 — should NOT generate a warning."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        bracket_warns = [w for _, w in warnings if "bracket" in w.get("Issue", "").lower()]
        self.assertEqual(len(bracket_warns), 0,
            "Bracket notation is valid per UG C3 slide 10, should not warn")

    def test_fixes_have_old_new_values(self):
        """FIX entries should have Old and New keys with actual values."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        addr_fixes = [f for _, f in fixes if "address" in f.get("Issue", "").lower()
                      or "flat" in f.get("Issue", "").lower()
                      or "number" in f.get("Issue", "").lower()]
        self.assertTrue(len(addr_fixes) >= 1, "Should have address-related FIX entries")
        for f in addr_fixes:
            self.assertIn("Old", f)
            self.assertIn("New", f)

    def test_fix_entries_have_all_keys(self):
        """All FIX entries should have Row, Field, Old, New, Issue keys."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        for _, f in fixes:
            self.assertIn("Row", f, f"FIX missing Row: {f}")
            self.assertIn("Field", f, f"FIX missing Field: {f}")
            self.assertIn("Old", f, f"FIX missing Old: {f}")
            self.assertIn("New", f, f"FIX missing New: {f}")
            self.assertIn("Issue", f, f"FIX missing Issue: {f}")

    # --- Dual-number auto-bracketing tests ---

    def test_dual_number_comma_bracketed(self):
        """Row 51: '506, 10 Evelina Gardens' -> '[506], 10 Evelina Gardens'."""
        row = self._get_row("51")
        self.assertEqual(row["Address1"], "[506], 10 Evelina Gardens")

    def test_dual_number_space_bracketed(self):
        """Row 52: '506 10 Evelina Gardens' -> '[506], 10 Evelina Gardens'."""
        row = self._get_row("52")
        self.assertEqual(row["Address1"], "[506], 10 Evelina Gardens")

    def test_dual_number_split_bracketed(self):
        """Row 53: Addr1='506', Addr2='10 Evelina Gardens' -> Addr1='[506]'."""
        row = self._get_row("53")
        self.assertEqual(row["Address1"], "[506]")
        self.assertEqual(row["Address2"], "10 Evelina Gardens")

    def test_single_number_not_bracketed(self):
        """Row 54: '42 High Road' -> unchanged (second token not a digit)."""
        row = self._get_row("54")
        self.assertEqual(row["Address1"], "42 High Road")

    def test_already_bracketed_not_changed(self):
        """Row 55: '[506], 10 Evelina Gardens' -> unchanged."""
        row = self._get_row("55")
        self.assertEqual(row["Address1"], "[506], 10 Evelina Gardens")

    def test_dual_number_occupied_addr2(self):
        """Row 61: '506, 10 Evelina Gardens' with Addr2='London' -> bracket in Addr1, Addr2 unchanged."""
        row = self._get_row("61")
        self.assertEqual(row["Address1"], "[506], 10 Evelina Gardens")
        self.assertEqual(row["Address2"], "London")

    def test_bare_number_pair_not_bracketed(self):
        """Row 62: '506 10' -> unchanged (only 2 tokens, no road word)."""
        row = self._get_row("62")
        self.assertEqual(row["Address1"], "506 10")

    def test_dual_number_no_comma_warning(self):
        """Bracketed results should NOT trigger 'NEEDS MANUAL FIX' comma warning."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        bracket_comma_warns = [w for _, w in warnings
                               if "[506]" in w.get("Value", "")
                               and "comma" in w.get("Issue", "").lower()]
        self.assertEqual(len(bracket_comma_warns), 0,
            "Bracketed dual-number addresses should not trigger comma warnings")

    def test_dual_number_fix_logged(self):
        """FIX entries should exist for dual-number auto-bracketing."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        bracket_fixes = [f for _, f in fixes if "dual-number auto-bracketed" in f.get("Issue", "")]
        self.assertTrue(len(bracket_fixes) >= 1, "Dual-number bracketing should produce FIX entries")

    # --- Zero-pad flat number tests ---

    def test_flat_number_padded(self):
        """Row 56: 'Flat 1' at 22 Evelina Gardens -> 'Flat 01' (max width 2 from Flat 12)."""
        row = self._get_row("56")
        self.assertEqual(row["Address1"], "Flat 01")

    def test_flat_alphanumeric_padded(self):
        """Row 57: 'Flat 3A' at 22 Evelina Gardens -> 'Flat 03A'."""
        row = self._get_row("57")
        self.assertEqual(row["Address1"], "Flat 03A")

    def test_flat_max_width_unchanged(self):
        """Row 58: 'Flat 12' at 22 Evelina Gardens -> unchanged (already max width)."""
        row = self._get_row("58")
        self.assertEqual(row["Address1"], "Flat 12")

    def test_flat_alpha_not_padded(self):
        """Row 59: 'Flat A' -> unchanged (alpha, not numeric)."""
        row = self._get_row("59")
        self.assertEqual(row["Address1"], "Flat A")

    def test_flat_multi_word_not_padded(self):
        """Row 60: 'Flat Ground Floor' -> unchanged (multi-word, no numeric ID)."""
        row = self._get_row("60")
        self.assertEqual(row["Address1"], "Flat Ground Floor")

    def test_flat_padding_fix_logged(self):
        """FIX entries should exist for zero-padded flat numbers."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        pad_fixes = [f for _, f in fixes if "zero-padded" in f.get("Issue", "")]
        self.assertTrue(len(pad_fixes) >= 1, "Zero-padding should produce FIX entries")

    def test_existing_flats_not_padded(self):
        """Rows 27-29: 'Flat 1' at 88 Kilburn Lane should remain 'Flat 1' (max width=1)."""
        for rollno in ("27", "28", "29"):
            row = self._get_row(rollno)
            self.assertEqual(row["Address1"], "Flat 1",
                f"Row {rollno}: 'Flat 1' at 88 Kilburn Lane should not be padded (single-digit group)")

    # --- Studio prefix tests ---

    def test_studio_comma_split(self):
        """Row 71: 'Studio 3, 30 Chamberlayne Road' -> split."""
        row = self._get_row("71")
        self.assertEqual(row["Address1"], "Studio 3")
        self.assertEqual(row["Address2"], "30 Chamberlayne Road")

    def test_studio_comma_free_split(self):
        """Row 72: 'Studio 3 30 Chamberlayne Road' -> split."""
        row = self._get_row("72")
        self.assertEqual(row["Address1"], "Studio 3")
        self.assertEqual(row["Address2"], "30 Chamberlayne Road")

    def test_number_before_studio_reordered(self):
        """Row 73: '56 Studio 1' -> Addr1='Studio 1', Addr2='56'."""
        row = self._get_row("73")
        self.assertEqual(row["Address1"], "Studio 1")
        self.assertEqual(row["Address2"], "56")

    def test_studio_number_padded(self):
        """Row 74: 'Studio 1' at 22 Evelina Gardens -> 'Studio 01' (max width 2 from Studio 10)."""
        row = self._get_row("74")
        self.assertEqual(row["Address1"], "Studio 01")

    def test_studio_max_width_unchanged(self):
        """Row 75: 'Studio 10' at 22 Evelina Gardens -> unchanged (already max width)."""
        row = self._get_row("75")
        self.assertEqual(row["Address1"], "Studio 10")

    # --- Fix 4b: Building name with road suffix ---

    def test_court_reordered_with_road_in_addr2(self):
        """Row 76: '24 Sheil Court' with Addr2='30 Chamberlayne Road' -> 'Sheil Court 24'."""
        row = self._get_row("76")
        self.assertEqual(row["Address1"], "Sheil Court 24")
        self.assertEqual(row["Address2"], "30 Chamberlayne Road")

    def test_court_not_reordered_empty_addr2(self):
        """Row 77: '24 Sheil Court' with empty Addr2 -> unchanged (could be a road)."""
        row = self._get_row("77")
        self.assertEqual(row["Address1"], "24 Sheil Court")

    # --- Directional road suffix ---

    def test_road_with_direction_not_reordered(self):
        """Row 79: '73 Park Avenue North' -> unchanged (recognised as road + direction)."""
        row = self._get_row("79")
        self.assertEqual(row["Address1"], "73 Park Avenue North")

    # --- Building number zero-padding ---

    def test_building_number_padded(self):
        """Row 80: '3 Maple House' -> 'Maple House 03' (Fix 4 reorder + padding)."""
        row = self._get_row("80")
        self.assertEqual(row["Address1"], "Maple House 03")

    def test_building_number_max_width_unchanged(self):
        """Row 81: '14 Maple House' -> 'Maple House 14' (already max width)."""
        row = self._get_row("81")
        self.assertEqual(row["Address1"], "Maple House 14")

    def test_building_number_alphanumeric_padded(self):
        """Row 82: '1A Maple House' -> 'Maple House 01A' (padded with letter suffix)."""
        row = self._get_row("82")
        self.assertEqual(row["Address1"], "Maple House 01A")

    def test_building_number_leading_not_padded(self):
        """Row 83: '24 Sheil Court' no Addr2 -> unchanged (not reordered)."""
        row = self._get_row("83")
        self.assertEqual(row["Address1"], "24 Sheil Court")

    def test_building_number_single_not_padded(self):
        """Row 84: Only one Oak Lodge entry -> not padded (max width=1)."""
        row = self._get_row("84")
        self.assertEqual(row["Address1"], "Oak Lodge 5")

    def test_building_padding_fix_logged(self):
        """FIX entries should exist for zero-padded building numbers."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        bldg_pad_fixes = [f for _, f in fixes if "building number zero-padded" in f.get("Issue", "")]
        self.assertTrue(len(bldg_pad_fixes) >= 1)

    # --- Fix 4c: Single-word building name with Address2 road ---

    def test_single_word_building_reordered(self):
        """Row 85: '26 Dorada' with Addr2='30 Chamberlayne Road' -> 'Dorada 26'."""
        row = self._get_row("85")
        self.assertEqual(row["Address1"], "Dorada 26")
        self.assertEqual(row["Address2"], "30 Chamberlayne Road")

    def test_single_word_building_zero_padded(self):
        """Row 86: '3 Dorada' with same road/postcode -> 'Dorada 03' (padded to match row 85)."""
        row = self._get_row("86")
        self.assertEqual(row["Address1"], "Dorada 03")

    def test_single_word_road_suffix_reordered(self):
        """Row 87: '26 Broadway' with Addr2='30 Chamberlayne Road' -> 'Broadway 26'."""
        row = self._get_row("87")
        self.assertEqual(row["Address1"], "Broadway 26")

    def test_single_word_no_addr2_not_reordered(self):
        """Row 88: '26 Dorada' with empty Addr2 -> unchanged (ambiguous)."""
        row = self._get_row("88")
        self.assertEqual(row["Address1"], "26 Dorada")

    def test_single_word_non_road_addr2_not_reordered(self):
        """Row 89: '26 Dorada' with Addr2='London' -> unchanged (Addr2 not a road)."""
        row = self._get_row("89")
        self.assertEqual(row["Address1"], "26 Dorada")

    def test_single_char_still_not_reordered(self):
        """Row 42: '14 B' with Addr2='Coleman Road' -> unchanged (single char guard)."""
        row = self._get_row("42")
        self.assertEqual(row["Address1"], "14 B")

    def test_single_word_alphanumeric_reordered(self):
        """Row 90: '26A Dorada' with Addr2='30 Chamberlayne Road' -> 'Dorada 26A'."""
        row = self._get_row("90")
        self.assertEqual(row["Address1"], "Dorada 26A")

    def test_single_word_building_fix_logged(self):
        """FIX entries should exist for single-word building name reordering."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        single_fixes = [f for _, f in fixes if "single-word building name reordered" in f.get("Issue", "")]
        self.assertTrue(len(single_fixes) >= 1)


# ---------------------------------------------------------------------------
# Realistic Messy Data Tests
# ---------------------------------------------------------------------------

class TestRealisticMessyData(unittest.TestCase):
    """Test conversion of realistic messy council data (132 rows, 4 PDs).

    Verifies that all UG C3 issues are handled correctly when mixed
    together in a naturalistic dataset, including name normalization
    and address auto-fixing.
    """

    @classmethod
    def setUpClass(cls):
        cls.input_file = TEST_DATA / "realistic_messy_council_data.csv"
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            cls.input_file, cls.tmp_output.name,
            extra_args=[],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

        # Read input for cross-checks
        with open(cls.input_file, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            cls.input_rows = list(reader)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"Failed:\n{self.stderr}")

    def test_row_count_accounting(self):
        """Input rows = output rows + deleted rows."""
        deletions = [l for l in self.machine if l.startswith("DELETED")]
        self.assertEqual(len(self.input_rows), len(self.rows) + len(deletions),
            f"Input ({len(self.input_rows)}) != output ({len(self.rows)}) "
            f"+ deleted ({len(deletions)})")

    # --- Deletion tests ---

    def test_exactly_three_deletions(self):
        """Three no-address electors should be deleted."""
        deletions = [l for l in self.machine if l.startswith("DELETED")]
        self.assertEqual(len(deletions), 3)

    def test_all_deletions_are_no_address(self):
        """Every deletion should have reason 'no address'."""
        for line in self.machine:
            if line.startswith("DELETED"):
                _, fields = parse_machine_line(line)
                self.assertEqual(fields["Reason"], "no address")

    def test_protected_electors_deleted(self):
        """The three protected/anonymous electors should be removed."""
        surnames = [r["Surname"] for r in self.rows]
        for name in ["Witness", "Anon", "Redacted"]:
            self.assertNotIn(name, surnames,
                f"No-address elector '{name}' should have been deleted")

    # --- Address issue flagging ---

    def test_ampersand_auto_replaced(self):
        """Ampersand in address should be auto-replaced with 'and'."""
        mensah = [r for r in self.rows if r["Surname"] == "Mensah"
                  and "and" in r.get("Address1", "")]
        self.assertTrue(len(mensah) >= 1, "At least one Mensah row should have 'and' in Address1")
        self.assertEqual(mensah[0]["Address1"], "1ST and 2ND")
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        amp_fixes = [f for _, f in fixes if "ampersand" in f.get("Issue", "").lower()]
        self.assertTrue(len(amp_fixes) >= 1, "Ampersand should produce FIX entry")

    def test_comma_building_road_flagged(self):
        """Comma building+road pattern should generate a NEEDS MANUAL FIX warning."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        comma_warns = [w for _, w in warnings if "comma" in w.get("Issue", "").lower()]
        self.assertTrue(len(comma_warns) >= 1, "Comma building+road should be flagged")
        for w in comma_warns:
            self.assertIn("NEEDS MANUAL FIX", w.get("Issue", ""))

    def test_invalid_addresses_auto_fixed(self):
        """Auto-fixable address patterns should be corrected in output."""
        # "14 South House" -> "South House 14" (number before building reordered)
        south_house = [r for r in self.rows if r["Surname"] == "Nowak"]
        self.assertEqual(len(south_house), 1)
        self.assertEqual(south_house[0]["Address1"], "South House 14")

        # "56 Flat 1" with Address2="Chamberlayne Road" -> reordered to "Flat 1" / "56 Chamberlayne Road"
        flat_issue = [r for r in self.rows if r["Surname"] == "Garcia"]
        self.assertEqual(len(flat_issue), 1)
        self.assertEqual(flat_issue[0]["Address1"], "Flat 1")
        self.assertEqual(flat_issue[0]["Address2"], "56 Chamberlayne Road")

        # Bracket notation should be in output (preserved as-is, flagged)
        bracket = [r for r in self.rows if r["Surname"] == "Ali"]
        self.assertEqual(len(bracket), 1)
        self.assertIn("[100-102]", bracket[0]["Address1"])

    def test_valid_addresses_preserved(self):
        """Valid address formats should pass through cleanly."""
        # "Flat 3, 30 Chamberlayne Road" with Address2="London" → NOT split (occupied)
        byrne = [r for r in self.rows if r["Surname"] == "Byrne"]
        self.assertEqual(len(byrne), 1)
        self.assertEqual(byrne[0]["Address1"], "Flat 3, 30 Chamberlayne Road")

        # "South House" / "21 Chamberlayne Road" (valid building name split)
        ahmed = [r for r in self.rows if r["Surname"] == "Ahmed"]
        self.assertEqual(len(ahmed), 2)
        self.assertEqual(ahmed[0]["Address1"], "South House")

    def test_address_gap_fixed(self):
        """Address gap (road in Address3, Address2 empty) should be shifted up."""
        singh = [r for r in self.rows if r["Surname"] == "Singh"]
        self.assertEqual(len(singh), 1)
        self.assertEqual(singh[0]["Address1"], "25")
        self.assertEqual(singh[0]["Address2"], "Chamberlayne Road")
        self.assertEqual(singh[0]["Address3"], "")

    # --- Date of Attainment ---

    def test_invalid_dates_cleared(self):
        """PENDING, N/A, and unreasonable years should be blanked."""
        fletcher = [r for r in self.rows if r["Surname"] == "Fletcher"]
        self.assertEqual(fletcher[0]["Date of Attainment"], "")

        daniels = [r for r in self.rows if r["Surname"] == "Daniels"]
        self.assertEqual(daniels[0]["Date of Attainment"], "")

        walker = [r for r in self.rows if r["Surname"] == "Walker"]
        self.assertEqual(walker[0]["Date of Attainment"], "")

    def test_iso_date_converted(self):
        """YYYY-MM-DD dates should be converted to DD/MM/YYYY."""
        kone = [r for r in self.rows if r["Surname"] == "Kone"]
        self.assertEqual(kone[0]["Date of Attainment"], "15/06/2008")

        opoku = [r for r in self.rows if r["Surname"] == "Opoku"]
        self.assertEqual(opoku[0]["Date of Attainment"], "15/05/2026")

    def test_dot_date_converted(self):
        """DD.MM.YYYY dates should be converted to DD/MM/YYYY."""
        jelani = [r for r in self.rows if r["Surname"] == "Jelani"]
        self.assertEqual(jelani[0]["Date of Attainment"], "01/06/2009")

    def test_valid_date_passthrough(self):
        """DD/MM/YYYY dates should pass through unchanged."""
        lewis = [r for r in self.rows if r["Surname"] == "Lewis"]
        self.assertEqual(lewis[0]["Date of Attainment"], "15/03/2008")

    def test_realistic_dob_passes(self):
        """A realistic-looking DOB (22/11/1985) is a valid date and should pass through.

        UG C3 says Date of Attainment is NOT the DOB, but our tool doesn't try
        to distinguish — it only validates format. Users must fix semantics.
        """
        okafor = [r for r in self.rows if r["Surname"] == "Okafor"
                  and r["Forename"] == "Adaeze"]
        self.assertEqual(len(okafor), 1)
        self.assertEqual(okafor[0]["Date of Attainment"], "22/11/1985")

    # --- Postcode normalization ---

    def test_postcode_spaces_normalized(self):
        """Extra spaces in postcodes should be collapsed."""
        brown = [r for r in self.rows if r["Surname"] == "Brown"]
        self.assertEqual(brown[0]["PostCode"], "NW10 3JU")

    def test_postcode_lowercase_uppercased(self):
        """Lowercase postcodes should be uppercased."""
        kamara = [r for r in self.rows if r["Surname"] == "Kamara"]
        self.assertEqual(kamara[0]["PostCode"], "NW10 3JU")

    def test_postcode_missing_space_fixed(self):
        """Postcodes like NW104UJ should get a space inserted."""
        amponsah = [r for r in self.rows if r["Surname"] == "Amponsah"]
        self.assertEqual(amponsah[0]["PostCode"], "NW10 4UJ")

    # --- Missing names ---

    def test_missing_forename_flagged_and_kept(self):
        """Missing forename should be flagged but kept."""
        abdi = [r for r in self.rows if r["Surname"] == "Abdi"]
        self.assertEqual(len(abdi), 1)
        self.assertEqual(abdi[0]["Forename"], "")
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        name_warns = [w for _, w in warnings if "forename" in w.get("Issue", "").lower()]
        self.assertTrue(len(name_warns) >= 1)

    def test_missing_surname_flagged_and_kept(self):
        """Missing surname should be flagged but kept."""
        victoria = [r for r in self.rows if r["Forename"] == "Victoria"
                    and r["Surname"] == ""]
        self.assertEqual(len(victoria), 1)
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        name_warns = [w for _, w in warnings if "surname" in w.get("Issue", "").lower()]
        self.assertTrue(len(name_warns) >= 1)

    # --- Duplicates ---

    def test_duplicate_flagged_and_kept(self):
        """Duplicate decimal RollNo should be normalized and both kept."""
        bakare = [r for r in self.rows if r["Surname"] == "Bakare"]
        self.assertEqual(len(bakare), 2, "Both duplicate rows should be in output")
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        suffix_fixes = [f for _, f in fixes if "suffix normalized" in f.get("Issue", "")]
        self.assertTrue(len(suffix_fixes) >= 1)

    # --- SubHouse/House ---

    def test_subhouse_house_preserved_in_output(self):
        """SubHouse/House columns should pass through to output as council columns."""
        self.assertIn("SubHouse", self.headers)
        self.assertIn("House", self.headers)

    def test_subhouse_composed_fernandez(self):
        """Fernandez rows have SubHouse folded into Address1, House into Address2."""
        fernandez = [r for r in self.rows if r["Surname"] == "Fernandez"]
        self.assertEqual(len(fernandez), 2)
        addr1s = sorted(r["Address1"] for r in fernandez)
        self.assertEqual(addr1s, ["Flat 2", "Flat 3"])
        for r in fernandez:
            self.assertEqual(r["Address2"], "Regency Court")
            # Council put "Regency Court" in RegisteredAddress1 too — that
            # duplicate is dropped, so Address3 carries the road.
            self.assertEqual(r["Address3"], "35 Chamberlayne Road")

    def test_subhouse_composed_rivera(self):
        """Rivera row has SubHouse=Flat 9 folded into Address1."""
        rivera = [r for r in self.rows if r["Surname"] == "Rivera"]
        self.assertEqual(len(rivera), 1)
        self.assertEqual(rivera[0]["Address1"], "Flat 9")
        self.assertEqual(rivera[0]["Address2"], "Kilburn Court")
        self.assertEqual(rivera[0]["Address3"], "10 Kilburn Lane")

    # --- Multi-elector addresses ---

    def test_families_at_same_address(self):
        """Multiple electors at the same address should all be in output."""
        patels = [r for r in self.rows if r["Surname"] == "Patel"]
        self.assertEqual(len(patels), 2)
        self.assertEqual(patels[0]["Address1"], patels[1]["Address1"])

        adus = [r for r in self.rows if r["Surname"] == "Adu"]
        self.assertEqual(len(adus), 4)

    def test_flat_block_all_present(self):
        """All electors in a flat block should be in output."""
        ontario = [r for r in self.rows
                   if "Ontario Point" in r.get("Address1", "")]
        self.assertTrue(len(ontario) >= 10,
            f"Expected 10+ Ontario Point electors, got {len(ontario)}")

    # --- Non-ASCII ---

    def test_non_ascii_names_preserved(self):
        """Non-ASCII characters should survive the conversion."""
        obrien = [r for r in self.rows if r["Surname"] == "O'Brien"]
        self.assertEqual(len(obrien), 1)

    # --- Decimal RollNo ---

    def test_decimal_rollno_normalized(self):
        """Decimal RollNo 35.5 should be split: Elector No.=35, suffix=1."""
        dos_santos = [r for r in self.rows if r["Surname"] == "Dos Santos"]
        self.assertEqual(len(dos_santos), 1)
        self.assertEqual(dos_santos[0]["Elector No."], "35")
        self.assertEqual(dos_santos[0]["Elector No. Suffix"], "1")

    # --- UPRN ---

    def test_uprn_passthrough(self):
        """UPRN values should pass through unchanged."""
        fofana = [r for r in self.rows if r["Surname"] == "Fofana"]
        self.assertEqual(len(fofana), 1)
        self.assertEqual(fofana[0]["UPRN"], "10070834521")

    # --- Postcode-only address ---

    def test_postcode_only_flagged(self):
        """Postcode with no street address should be flagged."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        addr_warns = [w for _, w in warnings if "PostCode present but no street" in w.get("Issue", "")]
        self.assertTrue(len(addr_warns) >= 1)

    # --- All 4 polling districts in output ---

    def test_all_polling_districts_present(self):
        """All 4 polling districts should be in output."""
        pds = set(r["Elector No. Prefix"] for r in self.rows)
        self.assertEqual(pds, {"KG1", "KG2", "HP1", "HP2"})

    # --- Output format ---

    def test_bom_and_crlf(self):
        """Output should have UTF-8 BOM and CRLF line endings."""
        with open(self.tmp_output.name, "rb") as f:
            data = f.read()
        self.assertTrue(data.startswith(b"\xef\xbb\xbf"))
        segments = data.split(b"\n")
        for seg in segments[:-1]:
            self.assertTrue(seg.endswith(b"\r"))

    def test_date_of_attainment_column_present(self):
        """Date of Attainment should be in output (since input has date data)."""
        self.assertIn("Date of Attainment", self.headers)

    def test_council_columns_preserved_in_output(self):
        """Non-mapped council columns should be preserved in output by default."""
        preserved = {"ElectorTitle", "IERStatus", "FranchiseMarker", "SubHouse", "House"}
        for col in preserved:
            self.assertIn(col, self.headers, f"Council column '{col}' should be preserved")
        # FIELD_MAP source columns should NOT be in output (mapped to TTW names)
        for col in ("RegisteredAddress1", "PDCode", "RollNo"):
            self.assertNotIn(col, self.headers, f"Mapped column '{col}' should not appear")

    # --- Name normalization in realistic data ---

    def test_all_caps_names_normalized(self):
        """ALL CAPS names should be normalized to title case."""
        johnson = [r for r in self.rows if r["Surname"] == "Johnson"
                   and r["Forename"] == "Michael"]
        self.assertEqual(len(johnson), 1, "JOHNSON/MICHAEL should be normalized to Johnson/Michael")

    def test_mc_prefix_normalized(self):
        """Mc prefix should be normalized: MCKENZIE -> McKenzie."""
        mckenzie = [r for r in self.rows if r["Surname"] == "McKenzie"]
        self.assertEqual(len(mckenzie), 1, "MCKENZIE should be normalized to McKenzie")

    def test_lowercase_names_normalized(self):
        """Lowercase names should be normalized: van der berg -> Van Der Berg."""
        vdb = [r for r in self.rows if r["Forename"] == "Anna"
               and r["Surname"].lower() == "van der berg"]
        self.assertEqual(len(vdb), 1)
        self.assertEqual(vdb[0]["Surname"], "Van Der Berg")

    def test_fix_count_in_report_summary(self):
        """Report summary should include fix count."""
        self.assertIn("Fixes applied", self.report_text)

    def test_address_fixes_in_report(self):
        """FIX lines should exist in machine-readable section."""
        fixes = [l for l in self.machine if l.startswith("FIX")]
        self.assertTrue(len(fixes) >= 1, "Should have at least one FIX entry")

    def test_jallow_ontario_point_reordered(self):
        """'11 Ontario Point' should be reordered to 'Ontario Point 11'."""
        jallow = [r for r in self.rows if r["Surname"] == "Jallow"]
        self.assertEqual(len(jallow), 1)
        self.assertEqual(jallow[0]["Address1"], "Ontario Point 11")

    def test_nowak_south_house_reordered(self):
        """'14 South House' should be reordered to 'South House 14'."""
        nowak = [r for r in self.rows if r["Surname"] == "Nowak"]
        self.assertEqual(len(nowak), 1)
        self.assertEqual(nowak[0]["Address1"], "South House 14")

    def test_garcia_56_flat_1_fixed(self):
        """'56 Flat 1' with Address2='Chamberlayne Road' should be reordered."""
        garcia = [r for r in self.rows if r["Surname"] == "Garcia"]
        self.assertEqual(len(garcia), 1)
        self.assertEqual(garcia[0]["Address1"], "Flat 1")
        self.assertEqual(garcia[0]["Address2"], "56 Chamberlayne Road")


# ---------------------------------------------------------------------------
# Suffix Normalize Tests
# ---------------------------------------------------------------------------

class TestSuffixNormalize(unittest.TestCase):
    """Test decimal RollNo normalization with enriched data."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"Failed:\n{self.stderr}")

    def test_fractional_rollnos_renumbered(self):
        """Decimal RollNo 10.5, 10.75, 10.875 -> Elector No.=10, suffixes 0, 1, 2."""
        kg2_10 = [r for r in self.rows if r["Elector No. Prefix"] == "KG2"
                  and r["Elector No."] == "10"]
        self.assertEqual(len(kg2_10), 3)
        suffixes = sorted(r["Elector No. Suffix"] for r in kg2_10)
        self.assertEqual(suffixes, ["0", "1", "2"])

    def test_primary_entry_gets_zero(self):
        """The smallest fractional RollNo becomes primary (suffix 0)."""
        kg1_3 = [r for r in self.rows if r["Elector No. Prefix"] == "KG1"
                 and r["Elector No."] == "3"]
        self.assertEqual(len(kg1_3), 2)
        # Patel Raj had RollNo 3.5 -> should be suffix 0 (smallest)
        raj = [r for r in kg1_3 if r["Forename"] == "Raj"]
        self.assertEqual(len(raj), 1)
        self.assertEqual(raj[0]["Elector No. Suffix"], "0")

    def test_single_row_suffix_zero(self):
        """Solo PDCode+RollNo should have suffix '0'."""
        smith = [r for r in self.rows if r["Surname"] == "Smith" and r["Forename"] == "John"]
        self.assertEqual(len(smith), 1)
        self.assertEqual(smith[0]["Elector No. Suffix"], "0")

    def test_full_elector_no_with_normalized_suffix(self):
        """Full Elector No. format should be Prefix-No-Suffix."""
        for r in self.rows:
            prefix = r["Elector No. Prefix"]
            number = r["Elector No."]
            suffix = r["Elector No. Suffix"]
            expected = f"{prefix}-{number}-{suffix}"
            self.assertEqual(r["Full Elector No."], expected,
                f"FEN mismatch: expected {expected}, got {r['Full Elector No.']}")

    def test_all_fractional_group(self):
        """When ALL rows have decimal RollNo, smallest fraction gets suffix 0."""
        kg2_10 = [r for r in self.rows if r["Elector No. Prefix"] == "KG2"
                  and r["Elector No."] == "10"]
        # O'Brien-Murphy had RollNo 10.5 -> should be suffix 0 (smallest)
        sean = [r for r in kg2_10 if r["Surname"] == "O'Brien-Murphy"]
        self.assertEqual(len(sean), 1)
        self.assertEqual(sean[0]["Elector No. Suffix"], "0")

    def test_suffix_renumber_logged(self):
        """Renumbering should appear as FIX entries in report."""
        fixes = [parse_machine_line(l) for l in self.machine if l.startswith("FIX")]
        suffix_fixes = [f for _, f in fixes if "suffix normalized" in f.get("Issue", "")
                        or "decimal RollNo" in f.get("Issue", "")]
        self.assertTrue(len(suffix_fixes) >= 1,
            "Suffix normalization should produce FIX entries")

    def test_normalize_guarantees_unique_fen(self):
        """Normalized suffixes should produce unique Full Elector No. values."""
        fens = [r["Full Elector No."] for r in self.rows]
        self.assertEqual(len(fens), len(set(fens)),
            f"Duplicate Full Elector No. values: {[f for f in fens if fens.count(f) > 1]}")



# ---------------------------------------------------------------------------
# Enriched Columns Tests
# ---------------------------------------------------------------------------

class TestEnrichedColumns(unittest.TestCase):
    """Test --enriched-columns election data mapping from GE24/Party/1-5."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def _get_row(self, surname, forename=None):
        matches = [r for r in self.rows if r["Surname"] == surname]
        if forename:
            matches = [r for r in matches if r["Forename"] == forename]
        self.assertTrue(len(matches) >= 1, f"Row {surname}/{forename} not found")
        return matches[0]

    def test_ge24_any_nonempty_becomes_voted(self):
        """GE24='yes' -> Voted='Y'."""
        row = self._get_row("Patel", "Raj")
        self.assertEqual(row["GE2024 Voted"], "Y")

    def test_ge24_blank_no_voted(self):
        """Blank GE24 -> Voted=''."""
        row = self._get_row("Smith", "John")
        self.assertEqual(row["GE2024 Voted"], "")

    def test_party_full_names_mapped(self):
        """Full party names should be mapped to TTW codes."""
        cases = [
            ("Smith", "John", "G"),          # Green
            ("Jones", "Sarah", "Lab"),        # Labour
            ("Patel", "Raj", "Con"),          # Conservative
            ("Williams", "Emma", "LD"),       # Liberal Democrat
            ("Martinez", "Ana", "REF"),       # Reform
        ]
        for surname, forename, expected in cases:
            row = self._get_row(surname, forename)
            self.assertEqual(row["LE2026 Party"], expected,
                f"{surname}: expected party '{expected}', got '{row['LE2026 Party']}'")

    def test_party_other_mapped(self):
        """'Other party' -> 'Oth'."""
        row = self._get_row("Garcia-Lopez", "Maria")
        self.assertEqual(row["LE2026 Party"], "Oth")

    def test_party_non_party_blanked(self):
        """'Did not vote', 'Won't say' -> blank."""
        kim = self._get_row("Kim", "Ji-Yeon")
        self.assertEqual(kim["LE2026 Party"], "")
        brown = self._get_row("Brown", "Michael")
        self.assertEqual(brown["LE2026 Party"], "")

    def test_party_ttw_code_passthrough(self):
        """Already-valid TTW code 'G' should pass through unchanged."""
        chen = self._get_row("Chen", "Wei")
        self.assertEqual(chen["LE2026 Party"], "G")

    def test_party_unrecognized_warned(self):
        """Unrecognized party kept as-is + warning."""
        taylor = self._get_row("Taylor", "Sophie")
        self.assertEqual(taylor["LE2026 Party"], "SomeParty")
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        party_warns = [w for _, w in warnings if "SomeParty" in w.get("Value", "")]
        self.assertTrue(len(party_warns) >= 1, "Unrecognized party should generate WARNING")

    def test_party_case_insensitive(self):
        """Party mapping should be case-insensitive: 'green', 'GREEN', 'Green' all -> 'G'."""
        # Test via the map_party_name function directly
        sys.path.insert(0, str(SCRIPT_DIR))
        from ttw_common import map_party_name
        for variant in ("green", "GREEN", "Green", "gReEn"):
            mapped, warning = map_party_name(variant)
            self.assertEqual(mapped, "G",
                f"map_party_name({variant!r}) should return 'G', got {mapped!r}")
            self.assertIsNone(warning)

    def test_1_5_becomes_gvi(self):
        """'1-5' column values map to Green Voting Intention on future election."""
        smith = self._get_row("Smith", "John")
        self.assertEqual(smith["LE2026 Green Voting Intention"], "1")
        jones = self._get_row("Jones", "Sarah")
        self.assertEqual(jones["LE2026 Green Voting Intention"], "3")

    def test_1_5_invalid_warned(self):
        """Value outside 1-5 should generate warning and be cleared."""
        taylor = self._get_row("Taylor", "Sophie")
        self.assertEqual(taylor["LE2026 Green Voting Intention"], "")
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        gvi_warns = [w for _, w in warnings if "voting intention" in w.get("Issue", "").lower()]
        self.assertTrue(len(gvi_warns) >= 1)

    def test_postal_voter_mapped(self):
        """PostalVoter? values should map to LE2026 Postal Voter."""
        self.assertIn("LE2026 Postal Voter", self.headers)
        # Rows with PostalVoter? set should have "Y"
        patel_raj = self._get_row("Patel", "Raj")
        self.assertEqual(patel_raj["LE2026 Postal Voter"], "Y")
        johnson_david = self._get_row("Johnson", "David")
        self.assertEqual(johnson_david["LE2026 Postal Voter"], "Y")
        # Rows without PostalVoter? should be empty
        smith = self._get_row("Smith", "John")
        self.assertEqual(smith["LE2026 Postal Voter"], "")

    def test_postal_voter_any_nonempty_becomes_Y(self):
        """Any non-empty PostalVoter? value should become 'Y'."""
        # Taylor has PostalVoter?="PV" -> should be "Y"
        taylor = self._get_row("Taylor", "Sophie")
        self.assertEqual(taylor["LE2026 Postal Voter"], "Y")
        # Patel Priya has PostalVoter?="v" -> "Y"
        priya = self._get_row("Patel", "Priya")
        self.assertEqual(priya["LE2026 Postal Voter"], "Y")

    def test_ppb_does_not_affect_postal_voter(self):
        """P/PB column (Poster/Poster Board) should not map to Postal Voter."""
        # P/PB is preserved as an extra column, not used for postal voter
        self.assertIn("P/PB", self.headers)
        # Jones has no PostalVoter? -> postal voter should be empty
        # regardless of any P/PB value
        jones = self._get_row("Jones", "Sarah")
        self.assertEqual(jones["LE2026 Postal Voter"], "")

    def test_historic_election_no_gvi_party_columns(self):
        """Historic election should NOT have GVI or Party columns in enriched mode."""
        self.assertNotIn("GE2024 Green Voting Intention", self.headers)
        self.assertNotIn("GE2024 Party", self.headers)
        # But Voted should still be there
        self.assertIn("GE2024 Voted", self.headers)

    def test_multiple_historic_elections_errors(self):
        """--enriched-columns with 2+ historic elections should error."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "GE2019",
                "--election-types", "historic", "historic",
                "--enriched-columns",
            ],
        )
        os.unlink(tmp_out.name)
        self.assertNotEqual(rc, 0, "Should error with 2 historic elections")
        self.assertIn("one historic election", stderr.lower())

    def test_no_future_election_errors(self):
        """--enriched-columns with no future election should error."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024",
                "--election-types", "historic",
                "--enriched-columns",
            ],
        )
        os.unlink(tmp_out.name)
        self.assertNotEqual(rc, 0, "Should error with no future election")
        self.assertIn("future election", stderr.lower())

    def test_multiple_future_elections_errors(self):
        """--enriched-columns with 2+ future elections should error."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026", "BYE2026",
                "--election-types", "historic", "future", "future",
                "--enriched-columns",
            ],
        )
        os.unlink(tmp_out.name)
        self.assertNotEqual(rc, 0, "Should error with 2 future elections")
        self.assertIn("future election", stderr.lower())


# ---------------------------------------------------------------------------
# Unrecognized Column Tests
# ---------------------------------------------------------------------------

class TestUnrecognizedColumns(unittest.TestCase):
    """Test unrecognized input column detection and reporting."""

    def test_unknown_column_in_report(self):
        """Unknown input column should appear in QA report as unrecognized."""
        # Create a CSV with one unknown column
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False,
                                             newline="", encoding="utf-8")
        import csv as csv_mod
        writer = csv_mod.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname", "ElectorForename",
                         "RegisteredAddress1", "PostCode", "MysteryColumn"])
        writer.writerow(["KG1", "1", "Test", "User", "1 Test St", "NW10 1AA", "hello"])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()

        try:
            rc, _, _ = run_clean(tmp_in.name, tmp_out.name, report_file=tmp_report.name)
            self.assertEqual(rc, 0)
            report_text = Path(tmp_report.name).read_text(encoding="utf-8")
            self.assertIn("Unrecognized Input Columns (preserved)", report_text)
            self.assertIn("MysteryColumn", report_text)
        finally:
            os.unlink(tmp_in.name)
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)

    def test_full_name_lowercase_not_unrecognized(self):
        """'Full name' (lowercase n) should be recognized via ENRICHMENT_DISCARD_COLUMNS."""
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False,
                                             newline="", encoding="utf-8")
        import csv as csv_mod
        writer = csv_mod.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname", "ElectorForename",
                         "RegisteredAddress1", "PostCode", "Full name",
                         "GE24", "Party", "1-5", "PostalVoter?"])
        writer.writerow(["KG1", "1", "Test", "User", "1 Test St", "NW10 1AA",
                         "User Test", "", "Green", "1", ""])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()

        try:
            rc, _, _ = run_clean(tmp_in.name, tmp_out.name,
                                 extra_args=["--mode", "register+elections",
                                             "--elections", "GE2024", "LE2026",
                                             "--election-types", "historic", "future",
                                             "--enriched-columns"],
                                 report_file=tmp_report.name)
            self.assertEqual(rc, 0)
            report_text = Path(tmp_report.name).read_text(encoding="utf-8")
            # "Full name" should NOT appear as unrecognized
            if "Unrecognized Input Columns" in report_text:
                self.assertNotIn("Full name", report_text)
        finally:
            os.unlink(tmp_in.name)
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)

    def test_enriched_no_false_positives(self):
        """Standard enriched input should produce no unrecognized column warnings."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()

        try:
            rc, _, _ = run_clean(
                TEST_DATA / "enriched_council_input.csv", tmp_out.name,
                extra_args=["--mode", "register+elections",
                            "--elections", "GE2024", "LE2026",
                            "--election-types", "historic", "future",
                            "--enriched-columns"],
                report_file=tmp_report.name)
            self.assertEqual(rc, 0)
            report_text = Path(tmp_report.name).read_text(encoding="utf-8")
            self.assertNotIn("Unrecognized Input Columns", report_text)
        finally:
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)

    def test_register_plus_elections_no_false_positives(self):
        """Non-enriched register+elections should not flag election columns."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()

        try:
            rc, _, _ = run_clean(
                TEST_DATA / "golden_input_register_plus_elections.csv", tmp_out.name,
                extra_args=["--mode", "register+elections",
                            "--elections", "2022", "2026",
                            "--election-types", "historic", "future"],
                report_file=tmp_report.name)
            self.assertEqual(rc, 0)
            report_text = Path(tmp_report.name).read_text(encoding="utf-8")
            self.assertNotIn("Unrecognized Input Columns", report_text)
        finally:
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)


# ---------------------------------------------------------------------------
# Extra Columns Tests
# ---------------------------------------------------------------------------

class TestExtraColumns(unittest.TestCase):
    """Test extra column passthrough and --strip-extra."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)

    def test_email_phone_in_output(self):
        """Email Address and Phone number columns should be present."""
        self.assertIn("Email Address", self.headers)
        self.assertIn("Phone number", self.headers)

    def test_dnk_ppb_preserved(self):
        """DNK and P/PB values should be preserved."""
        self.assertIn("DNK", self.headers)
        self.assertIn("P/PB", self.headers)
        jones = [r for r in self.rows if r["Surname"] == "Jones"]
        self.assertEqual(len(jones), 1)
        self.assertEqual(jones[0]["DNK"], "Do not knock")

    def test_identifier_columns_preserved(self):
        """Identifier and Address Identifier should pass through."""
        self.assertIn("Identifier", self.headers)
        self.assertIn("Address Identifier", self.headers)
        smith = [r for r in self.rows if r["Surname"] == "Smith" and r["Forename"] == "John"]
        self.assertEqual(smith[0]["Identifier"], "ID001")
        self.assertEqual(smith[0]["Address Identifier"], "ADDR001")

    def test_strip_extra_removes_all_extras(self):
        """--strip-extra should remove all non-TTW/election columns."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
                "--strip-extra",
            ],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)

        extra_cols = {"Email Address", "Phone number", "Comments", "Issues",
                      "P/PB", "DNK", "New", "1st round",
                      "Identifier", "Address Identifier"}
        leaked = extra_cols & set(headers)
        self.assertEqual(leaked, set(),
            f"Extra columns should be stripped: {leaked}")
        # Core + election columns should still be present
        self.assertIn("Elector No. Prefix", headers)
        self.assertIn("LE2026 Party", headers)


# ---------------------------------------------------------------------------
# Preserve-All-Columns Tests
# ---------------------------------------------------------------------------

class TestPreserveAllColumns(unittest.TestCase):
    """Test that all input columns are preserved by default and --strip-extra works."""

    def test_default_preserves_all_columns(self):
        """Running with just input/output preserves all input columns in output."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=[],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        # All council-only columns should be in output
        for col in ["ElectorTitle", "IERStatus", "FranchiseMarker",
                     "Euro", "Parl", "County", "Ward",
                     "MethodOfVerification", "ElectorID"]:
            self.assertIn(col, headers, f"Column '{col}' should be preserved by default")

    def test_strip_extra_drops_non_ttw(self):
        """--strip-extra produces only TTW columns (no extras)."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=["--strip-extra"],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        # No council-only columns should remain
        for col in ["ElectorTitle", "IERStatus", "FranchiseMarker",
                     "Euro", "Parl", "County", "Ward",
                     "MethodOfVerification", "ElectorID",
                     "SubHouse", "House", "Suffix"]:
            self.assertNotIn(col, headers, f"Column '{col}' should be stripped")
        # Core TTW columns should still be present
        self.assertIn("Elector No. Prefix", headers)
        self.assertIn("PostCode", headers)

    def test_strip_extra_without_enriched(self):
        """--strip-extra works in plain register mode (no --enriched-columns)."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_plus_elections.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "2022", "2026",
                "--election-types", "historic", "future",
                "--strip-extra",
            ],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        # No council-only columns
        for col in ["ElectorTitle", "IERStatus", "FranchiseMarker",
                     "Euro", "Parl", "County", "Ward"]:
            self.assertNotIn(col, headers, f"Column '{col}' should be stripped")
        # Election columns should be present
        self.assertIn("2022 Voted", headers)
        self.assertIn("2026 Postal Voter", headers)

    def test_enriched_source_consumed(self):
        """With --enriched-columns, GE24/Party/1-5/PostalVoter? NOT in output."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        for col in ["GE24", "Party", "1-5", "PostalVoter?"]:
            self.assertNotIn(col, headers,
                f"Enrichment source column '{col}' should be consumed by mapping")

    def test_enriched_extra_preserved(self):
        """With --enriched-columns (no --strip-extra), Email/Phone/etc preserved."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        for col in ["Email Address", "Phone number", "Comments", "Issues"]:
            self.assertIn(col, headers, f"Extra column '{col}' should be preserved")

    def test_council_only_preserved(self):
        """Euro, Parl, Ward, FranchiseMarker etc. in output by default."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "realistic_messy_council_data.csv", tmp_out.name,
            extra_args=[],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        for col in ["Euro", "Parl", "Ward", "FranchiseMarker"]:
            self.assertIn(col, headers, f"Council column '{col}' should be preserved")

    def test_strip_extra_with_enriched(self):
        """--enriched-columns --strip-extra -> only TTW + election columns."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
                "--strip-extra",
            ],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        # No extra columns
        for col in ["Email Address", "Phone number", "Euro", "Parl",
                     "Ward", "Full Name", "SubHouse", "House"]:
            self.assertNotIn(col, headers, f"Column '{col}' should be stripped")
        # TTW + election columns should still be present
        self.assertIn("Elector No. Prefix", headers)
        self.assertIn("GE2024 Voted", headers)
        self.assertIn("LE2026 Party", headers)

    def test_enrichment_detected_warning(self):
        """Without --enriched-columns, stderr warns about GE24/Party/1-5."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        # Run without --enriched-columns on enriched data (no --quiet)
        cmd = [sys.executable, str(TOOL),
               str(TEST_DATA / "enriched_council_input.csv"), tmp_out.name]
        result = subprocess.run(cmd, capture_output=True, text=True)
        os.unlink(tmp_out.name)
        self.assertEqual(result.returncode, 0)
        self.assertIn("Detected enrichment data columns", result.stderr)
        self.assertIn("--enriched-columns", result.stderr)

    def test_canvassing_detected_note(self):
        """Canvassing data detected -> stderr note."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        cmd = [sys.executable, str(TOOL),
               str(TEST_DATA / "enriched_council_input.csv"), tmp_out.name]
        result = subprocess.run(cmd, capture_output=True, text=True)
        os.unlink(tmp_out.name)
        self.assertEqual(result.returncode, 0)
        self.assertIn("Detected canvassing data columns", result.stderr)

    def test_no_enrichment_warning_when_enriched_set(self):
        """With --enriched-columns, no enrichment warning."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        cmd = [sys.executable, str(TOOL),
               str(TEST_DATA / "enriched_council_input.csv"), tmp_out.name,
               "--mode", "register+elections",
               "--elections", "GE2024", "LE2026",
               "--election-types", "historic", "future",
               "--enriched-columns"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        os.unlink(tmp_out.name)
        self.assertEqual(result.returncode, 0)
        self.assertNotIn("Detected enrichment data columns", result.stderr)

    def test_extra_columns_in_input_order(self):
        """Extra columns appear at end of output in their original input order."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=[],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        # Find positions of extra columns
        extras = [h for h in headers if h in {"ElectorTitle", "IERStatus",
                  "FranchiseMarker", "Euro", "Parl", "County", "Ward"}]
        # The input order is: ElectorTitle, IERStatus, FranchiseMarker, Euro, Parl, County, Ward
        expected_order = ["ElectorTitle", "IERStatus", "FranchiseMarker",
                         "Euro", "Parl", "County", "Ward"]
        self.assertEqual(extras, expected_order,
            f"Extra columns should be in input order.\nExpected: {expected_order}\nGot: {extras}")

    def test_ttw_named_input_column_no_overwrite(self):
        """An input column named like a TTW output field should not overwrite mapped values."""
        import csv as csv_mod
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False,
                                             newline="", encoding="utf-8")
        writer = csv_mod.writer(tmp_in)
        # Input has a column literally named "Address1" alongside RegisteredAddress1
        writer.writerow(["PDCode", "RollNo", "ElectorSurname", "ElectorForename",
                         "RegisteredAddress1", "PostCode", "Address1"])
        writer.writerow(["KG1", "1", "Test", "User", "42 Real Street", "NW10 1AA", "BOGUS"])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()

        try:
            rc, _, _ = run_clean(tmp_in.name, tmp_out.name)
            self.assertEqual(rc, 0)
            headers, rows = read_output_csv(tmp_out.name)
            # The mapped RegisteredAddress1 should win, not the raw "Address1" column
            self.assertEqual(rows[0]["Address1"], "42 Real Street")
            # "Address1" should not appear as a duplicate extra column
            self.assertEqual(headers.count("Address1"), 1)
        finally:
            os.unlink(tmp_in.name)
            os.unlink(tmp_out.name)

    def test_ttw_named_column_without_source_preserved(self):
        """An input column named 'Address2' should be preserved when RegisteredAddress2 is absent."""
        import csv as csv_mod
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False,
                                             newline="", encoding="utf-8")
        writer = csv_mod.writer(tmp_in)
        # Input uses "Address2" directly (no RegisteredAddress2)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname", "ElectorForename",
                         "RegisteredAddress1", "PostCode", "Address2"])
        writer.writerow(["KG1", "1", "Test", "User", "42 Real Street", "NW10 1AA", "London"])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()

        try:
            rc, _, _ = run_clean(tmp_in.name, tmp_out.name)
            self.assertEqual(rc, 0)
            headers, rows = read_output_csv(tmp_out.name)
            # Address2 data should NOT be silently dropped
            self.assertEqual(rows[0]["Address2"], "London",
                "Address2 data should be preserved when RegisteredAddress2 is not in input")
        finally:
            os.unlink(tmp_in.name)
            os.unlink(tmp_out.name)

    def test_strip_extra_discarded_in_report(self):
        """--strip-extra should list discarded columns in QA report."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()

        try:
            rc, _, _ = run_clean(
                TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
                extra_args=["--strip-extra"],
                report_file=tmp_report.name,
            )
            self.assertEqual(rc, 0)
            report_text = Path(tmp_report.name).read_text(encoding="utf-8")
            self.assertIn("Discarded columns", report_text)
            self.assertIn("ElectorTitle", report_text)
        finally:
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)

    def test_unrecognized_column_data_in_output(self):
        """Unrecognized column values should appear in output CSV rows."""
        import csv as csv_mod
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False,
                                             newline="", encoding="utf-8")
        writer = csv_mod.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname", "ElectorForename",
                         "RegisteredAddress1", "PostCode", "MysteryColumn"])
        writer.writerow(["KG1", "1", "Test", "User", "1 Test St", "NW10 1AA", "hello"])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()

        try:
            rc, _, _ = run_clean(tmp_in.name, tmp_out.name)
            self.assertEqual(rc, 0)
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("MysteryColumn", headers)
            self.assertEqual(rows[0]["MysteryColumn"], "hello")
        finally:
            os.unlink(tmp_in.name)
            os.unlink(tmp_out.name)

    def test_strip_extra_unrecognized_report_label(self):
        """With --strip-extra, report should label unrecognized columns as 'stripped'."""
        import csv as csv_mod
        tmp_in = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False,
                                             newline="", encoding="utf-8")
        writer = csv_mod.writer(tmp_in)
        writer.writerow(["PDCode", "RollNo", "ElectorSurname", "ElectorForename",
                         "RegisteredAddress1", "PostCode", "MysteryColumn"])
        writer.writerow(["KG1", "1", "Test", "User", "1 Test St", "NW10 1AA", "hello"])
        tmp_in.close()

        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()

        try:
            rc, _, _ = run_clean(tmp_in.name, tmp_out.name,
                                 extra_args=["--strip-extra"],
                                 report_file=tmp_report.name)
            self.assertEqual(rc, 0)
            report_text = Path(tmp_report.name).read_text(encoding="utf-8")
            self.assertIn("Unrecognized Input Columns (stripped)", report_text)
            self.assertNotIn("Unrecognized Input Columns (preserved)", report_text)
        finally:
            os.unlink(tmp_in.name)
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)


# ---------------------------------------------------------------------------
# Backwards Compatibility Tests
# ---------------------------------------------------------------------------

class TestBackwardsCompatibility(unittest.TestCase):
    """Test that existing functionality is not broken by enrichment changes."""

    def test_plain_council_still_works(self):
        """Existing council-format input without enrichment columns works as before."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=[],
        )
        _, rows = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        self.assertTrue(len(rows) > 0)

    def test_enriched_columns_flag_ignored_without_data(self):
        """--enriched-columns on plain council data (with election cols) doesn't crash."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_plus_elections.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "2022", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
        )
        _, rows = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(rc, 0, f"Should not crash:\n{stderr}")
        self.assertTrue(len(rows) > 0)

    def test_enriched_columns_without_mode_errors(self):
        """--enriched-columns without --mode register+elections should error."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "enriched_council_input.csv", tmp_out.name,
            extra_args=["--enriched-columns"],
        )
        os.unlink(tmp_out.name)
        self.assertNotEqual(rc, 0, "Should error without register+elections mode")

    def test_existing_golden_register_only_unchanged(self):
        """Existing golden file test still passes (register only)."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=[],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        exp_h, exp_r = read_output_csv(TEST_DATA / "golden_expected_register_only.csv")
        got_h, got_r = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(exp_h, got_h)
        self.assertEqual(len(exp_r), len(got_r))

    def test_existing_golden_register_plus_elections_unchanged(self):
        """Existing golden file test still passes (register+elections)."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, stderr = run_clean(
            TEST_DATA / "golden_input_register_plus_elections.csv", tmp_out.name,
            extra_args=["--mode", "register+elections",
                "--elections", "2022", "2026",
                "--election-types", "historic", "future",
            ],
        )
        self.assertEqual(rc, 0, f"Failed:\n{stderr}")
        exp_h, exp_r = read_output_csv(TEST_DATA / "golden_expected_register_plus_elections.csv")
        got_h, got_r = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(exp_h, got_h)
        self.assertEqual(len(exp_r), len(got_r))

    def test_enrich_register_tests_still_pass(self):
        """Verify test_enrichment.py passes after import refactor."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "test_enrichment.py")],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0,
            f"test_enrichment.py failed:\n{result.stderr}")


# ---------------------------------------------------------------------------
# Enriched Golden File Test
# ---------------------------------------------------------------------------

class TestEnrichedGoldenFile(unittest.TestCase):
    """Test full pipeline with enriched input against golden expected output."""

    @classmethod
    def setUpClass(cls):
        cls.golden_input = TEST_DATA / "enriched_council_input.csv"
        cls.golden_expected = TEST_DATA / "enriched_council_expected.csv"
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, cls.stdout, cls.stderr = run_clean(
            cls.golden_input, cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
            report_file=cls.tmp_report.name,
        )

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"clean_register.py failed:\n{self.stderr}")

    def test_header_match(self):
        exp_h, _ = read_output_csv(self.golden_expected)
        got_h, _ = read_output_csv(self.tmp_output.name)
        self.assertEqual(exp_h, got_h,
            f"Column order mismatch.\nExpected: {exp_h}\nGot:      {got_h}")

    def test_row_count(self):
        _, exp_r = read_output_csv(self.golden_expected)
        _, got_r = read_output_csv(self.tmp_output.name)
        self.assertEqual(len(exp_r), len(got_r))

    def test_field_level_match(self):
        exp_h, exp_r = read_output_csv(self.golden_expected)
        _, got_r = read_output_csv(self.tmp_output.name)
        for i, (exp, got) in enumerate(zip(exp_r, got_r)):
            for col in exp_h:
                self.assertEqual(
                    exp.get(col, ""), got.get(col, ""),
                    f"Row {i+1}, column '{col}': expected {repr(exp.get(col, ''))}, "
                    f"got {repr(got.get(col, ''))}"
                )

    def test_one_deletion(self):
        """NoAddress row should be deleted."""
        _, machine = read_report(self.tmp_report.name)
        deletions = [l for l in machine if l.startswith("DELETED")]
        self.assertEqual(len(deletions), 1)
        _, fields = parse_machine_line(deletions[0])
        self.assertEqual(fields["Reason"], "no address")


# ---------------------------------------------------------------------------
# Dataset 1 Golden File Test (Full Name uppercase, no PostalVoter?)
# ---------------------------------------------------------------------------

class TestDataset1GoldenFile(unittest.TestCase):
    """Test Dataset 1 format: council+enrichment with Full Name, no PostalVoter?."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "dataset1_input.csv", cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def _get_row(self, surname, forename=None):
        matches = [r for r in self.rows if r["Surname"] == surname]
        if forename:
            matches = [r for r in matches if r["Forename"] == forename]
        self.assertTrue(len(matches) >= 1, f"Row {surname}/{forename} not found")
        return matches[0]

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"Failed:\n{self.stderr}")

    def test_field_level_match(self):
        """Output matches golden expected file field-by-field."""
        exp_h, exp_r = read_output_csv(TEST_DATA / "dataset1_expected.csv")
        got_h, got_r = read_output_csv(self.tmp_output.name)
        self.assertEqual(exp_h, got_h)
        self.assertEqual(len(exp_r), len(got_r))
        for i, (exp, got) in enumerate(zip(exp_r, got_r)):
            for col in exp_h:
                self.assertEqual(exp.get(col, ""), got.get(col, ""),
                    f"Row {i+1}, column '{col}': expected {repr(exp.get(col, ''))}, "
                    f"got {repr(got.get(col, ''))}")

    def test_voted_Y_format(self):
        """GE24='yes' -> GE2024 Voted='Y' (not 'v')."""
        smith = self._get_row("Smith", "John")
        self.assertEqual(smith["GE2024 Voted"], "Y")

    def test_voted_N_becomes_blank(self):
        """GE24='N' -> GE2024 Voted='' (explicit no)."""
        priya = self._get_row("Patel", "Priya")
        self.assertEqual(priya["GE2024 Voted"], "")

    def test_postal_voter_blank_without_source(self):
        """No PostalVoter? column -> LE2026 Postal Voter always blank."""
        for row in self.rows:
            self.assertEqual(row["LE2026 Postal Voter"], "",
                f"{row['Surname']}: Postal Voter should be blank, got {row['LE2026 Postal Voter']!r}")

    def test_ppb_preserved(self):
        """P/PB values pass through as extra columns."""
        patel = self._get_row("Patel", "Raj")
        self.assertEqual(patel["P/PB"], "P")
        brown = self._get_row("Brown", "Michael")
        self.assertEqual(brown["P/PB"], "PB")

    def test_dnk_preserved(self):
        """DNK values pass through."""
        jones = self._get_row("Jones", "Sarah")
        self.assertEqual(jones["DNK"], "Do not knock")

    def test_no_unrecognized_columns(self):
        """No unrecognized column warnings in report."""
        self.assertNotIn("Unrecognized Input Columns", self.report_text)

    def test_one_deletion(self):
        """Ghost NoAddress row deleted."""
        deletions = [l for l in self.machine if l.startswith("DELETED")]
        self.assertEqual(len(deletions), 1)


# ---------------------------------------------------------------------------
# Dataset 2 Golden File Test (Full name lowercase, with PostalVoter?)
# ---------------------------------------------------------------------------

class TestDataset2GoldenFile(unittest.TestCase):
    """Test Dataset 2 format: enrichment with PostalVoter?, Full name lowercase."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_output = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        cls.tmp_output.close()
        cls.tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        cls.tmp_report.close()

        cls.rc, _, cls.stderr = run_clean(
            TEST_DATA / "dataset2_input.csv", cls.tmp_output.name,
            extra_args=["--mode", "register+elections",
                "--elections", "GE2024", "LE2026",
                "--election-types", "historic", "future",
                "--enriched-columns",
            ],
            report_file=cls.tmp_report.name,
        )
        cls.headers, cls.rows = read_output_csv(cls.tmp_output.name)
        cls.report_text, cls.machine = read_report(cls.tmp_report.name)

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.tmp_output.name)
        os.unlink(cls.tmp_report.name)

    def _get_row(self, surname, forename=None):
        matches = [r for r in self.rows if r["Surname"] == surname]
        if forename:
            matches = [r for r in matches if r["Forename"] == forename]
        self.assertTrue(len(matches) >= 1, f"Row {surname}/{forename} not found")
        return matches[0]

    def test_exit_code_zero(self):
        self.assertEqual(self.rc, 0, f"Failed:\n{self.stderr}")

    def test_field_level_match(self):
        """Output matches golden expected file field-by-field."""
        exp_h, exp_r = read_output_csv(TEST_DATA / "dataset2_expected.csv")
        got_h, got_r = read_output_csv(self.tmp_output.name)
        self.assertEqual(exp_h, got_h)
        self.assertEqual(len(exp_r), len(got_r))
        for i, (exp, got) in enumerate(zip(exp_r, got_r)):
            for col in exp_h:
                self.assertEqual(exp.get(col, ""), got.get(col, ""),
                    f"Row {i+1}, column '{col}': expected {repr(exp.get(col, ''))}, "
                    f"got {repr(got.get(col, ''))}")

    def test_postal_voter_yes_becomes_Y(self):
        """PostalVoter?='Yes' -> LE2026 Postal Voter='Y'."""
        patel = self._get_row("Patel", "Raj")
        self.assertEqual(patel["LE2026 Postal Voter"], "Y")

    def test_postal_voter_v_becomes_Y(self):
        """PostalVoter?='v' -> LE2026 Postal Voter='Y'."""
        brown = self._get_row("Brown", "Michael")
        self.assertEqual(brown["LE2026 Postal Voter"], "Y")

    def test_postal_voter_No_becomes_blank(self):
        """PostalVoter?='No' -> LE2026 Postal Voter='' (explicit no)."""
        priya = self._get_row("Patel", "Priya")
        self.assertEqual(priya["LE2026 Postal Voter"], "")

    def test_postal_voter_N_becomes_blank(self):
        """PostalVoter?='N' -> LE2026 Postal Voter='' (explicit no)."""
        garcia = self._get_row("Garcia-Lopez", "Maria")
        self.assertEqual(garcia["LE2026 Postal Voter"], "")

    def test_voted_N_becomes_blank(self):
        """GE24='N' -> GE2024 Voted='' (explicit no)."""
        priya = self._get_row("Patel", "Priya")
        self.assertEqual(priya["GE2024 Voted"], "")

    def test_voted_yes_becomes_Y(self):
        """GE24='yes' -> GE2024 Voted='Y'."""
        smith = self._get_row("Smith", "John")
        self.assertEqual(smith["GE2024 Voted"], "Y")

    def test_full_name_lowercase_not_unrecognized(self):
        """'Full name' (lowercase n) should not trigger unrecognized warning."""
        self.assertNotIn("Unrecognized Input Columns", self.report_text)

    def test_no_identifier_columns(self):
        """Dataset 2 has no Identifier/Address Identifier columns."""
        self.assertNotIn("Identifier", self.headers)
        self.assertNotIn("Address Identifier", self.headers)

    def test_one_deletion(self):
        """Ghost NoAddress row deleted."""
        deletions = [l for l in self.machine if l.startswith("DELETED")]
        self.assertEqual(len(deletions), 1)


# ---------------------------------------------------------------------------
# User Verification Mode
# ---------------------------------------------------------------------------

def run_verification(args):
    """Run verification checks on user-provided files."""
    print("=" * 60)
    print("Electoral Register Conversion Verification")
    print("=" * 60)
    print()

    results = {"pass": 0, "fail": 0, "warn": 0}
    MAX_DETAIL = 10  # max examples to show per check

    def report(status, label, detail="", examples=None):
        tag = {"pass": "[PASS]", "fail": "[FAIL]", "warn": "[WARN]"}[status]
        results[status] += 1
        msg = f"{tag} {label}"
        if detail:
            msg += f": {detail}"
        print(msg)
        if examples and status in ("fail", "warn"):
            shown = examples[:MAX_DETAIL]
            for ex in shown:
                print(f"       {ex}")
            if len(examples) > MAX_DETAIL:
                print(f"       ... and {len(examples) - MAX_DETAIL} more")

    # Read output file
    try:
        out_headers, out_rows = read_output_csv(args.output)
    except Exception as e:
        report("fail", "Output File", f"Cannot read: {e}")
        return results

    report("pass", "Output Readable", f"{len(out_rows)} rows, {len(out_headers)} columns")

    # Check BOM
    data = open(args.output, "rb").read()
    if data.startswith(b"\xef\xbb\xbf"):
        report("pass", "BOM Present", "UTF-8 BOM detected")
    else:
        report("fail", "BOM Present", "Missing UTF-8 BOM")

    # Check CRLF
    segments = data.split(b"\n")
    lf_only = sum(1 for s in segments[:-1] if not s.endswith(b"\r"))
    if lf_only == 0:
        report("pass", "CRLF Line Endings", "All lines use CRLF")
    else:
        report("fail", "CRLF Line Endings", f"{lf_only} lines with LF-only")

    # Check no mapped council columns leaked (columns that should have been renamed)
    # These are source columns in FIELD_MAP that should now be TTW names
    mapped_council_cols = {"PDCode", "RollNo", "ElectorSurname", "ElectorForename",
                           "ElectorMiddleName", "DateOfAttainment",
                           "RegisteredAddress1", "RegisteredAddress2",
                           "RegisteredAddress3", "RegisteredAddress4",
                           "RegisteredAddress5", "RegisteredAddress6"}
    # These are council-only columns that are deliberately preserved as extra columns
    preserved_council_cols = {"ElectorTitle", "IERStatus", "FranchiseMarker",
                              "SubHouse", "House", "MethodOfVerification", "ElectorID",
                              "Euro", "Parl", "County", "Ward", "ChangeTypeID"}
    leaked = mapped_council_cols & set(out_headers)
    preserved = preserved_council_cols & set(out_headers)
    if not leaked:
        msg = "Output uses TTW column names (no mapped columns leaked)"
        if preserved:
            msg += f". Preserved council columns: {preserved}"
        report("pass", "Column Mapping", msg)
    else:
        report("fail", "Column Mapping", f"Mapped council columns still in output: {leaked}")

    # Check required TTW columns
    required = {"Elector No. Prefix", "Elector No.", "Full Elector No.",
                "Surname", "Forename", "Address1", "PostCode"}
    missing = required - set(out_headers)
    if not missing:
        report("pass", "Required TTW Columns", "All present")
    else:
        report("fail", "Required TTW Columns", f"Missing: {missing}")

    # Check Full Elector No. format
    bad_fen = []
    for i, r in enumerate(out_rows):
        fen = r.get("Full Elector No.", "")
        parts = fen.split("-")
        if len(parts) != 3:
            bad_fen.append((i + 2, fen))
    if not bad_fen:
        report("pass", "Full Elector No. Format", "All match Prefix-No-Suffix")
    else:
        report("fail", "Full Elector No. Format",
            f"{len(bad_fen)} malformed",
            examples=[f"Row {row}: '{val}'" for row, val in bad_fen])

    # Check Full Elector No. uniqueness
    from collections import Counter
    fen_counts = Counter(r.get("Full Elector No.", "") for r in out_rows)
    dups = {k: v for k, v in fen_counts.items() if v > 1}
    if not dups:
        report("pass", "Unique Elector Numbers", f"All {len(out_rows)} Full Elector No. values unique")
    else:
        report("fail", "Unique Elector Numbers", f"{len(dups)} duplicates",
            examples=[f"'{k}' appears {v} times" for k, v in sorted(dups.items())])

    # Check election field values are in TTW format
    election_cols = [h for h in out_headers if h.endswith(" Voted")]
    bad_voted = []
    for i, r in enumerate(out_rows):
        for col in election_cols:
            val = r.get(col, "").strip()
            if val and val != "Y":
                bad_voted.append((i + 2, col, val))
    if election_cols:
        if not bad_voted:
            report("pass", "Voted Values", "All Voted values are 'Y' or blank")
        else:
            report("fail", "Voted Values",
                f"{len(bad_voted)} invalid (expected 'Y' or blank)",
                examples=[f"Row {row}: {col}='{val}'" for row, col, val in bad_voted])

    postal_cols = [h for h in out_headers if h.endswith(" Postal Voter")]
    bad_postal = []
    for i, r in enumerate(out_rows):
        for col in postal_cols:
            val = r.get(col, "").strip()
            if val and val != "Y":
                bad_postal.append((i + 2, col, val))
    if postal_cols:
        if not bad_postal:
            report("pass", "Postal Voter Values", "All Postal Voter values are 'Y' or blank")
        else:
            report("fail", "Postal Voter Values",
                f"{len(bad_postal)} invalid (expected 'Y' or blank)",
                examples=[f"Row {row}: {col}='{val}'" for row, col, val in bad_postal])

    party_cols = [h for h in out_headers if h.endswith(" Party")]
    sys.path.insert(0, str(SCRIPT_DIR))
    from ttw_common import VALID_PARTY_CODES
    bad_party = []
    for i, r in enumerate(out_rows):
        for col in party_cols:
            val = r.get(col, "").strip()
            if val and val not in VALID_PARTY_CODES:
                bad_party.append((i + 2, col, val))
    if party_cols:
        if not bad_party:
            report("pass", "Party Codes", "All Party values are valid TTW codes or blank")
        else:
            report("warn", "Party Codes",
                f"{len(bad_party)} unrecognized code(s)",
                examples=[f"Row {row}: {col}='{val}'" for row, col, val in bad_party])

    gvi_cols = [h for h in out_headers if h.endswith(" Green Voting Intention")]
    valid_gvi = {"1", "2", "3", "4", "5", ""}
    bad_gvi = []
    for i, r in enumerate(out_rows):
        for col in gvi_cols:
            val = r.get(col, "").strip()
            if val not in valid_gvi:
                bad_gvi.append((i + 2, col, val))
    if gvi_cols:
        if not bad_gvi:
            report("pass", "Voting Intention", "All GVI values are 1-5 or blank")
        else:
            report("fail", "Voting Intention",
                f"{len(bad_gvi)} invalid (expected 1-5 or blank)",
                examples=[f"Row {row}: {col}='{val}'" for row, col, val in bad_gvi])

    # Check no empty rows
    empty_rows = [i + 2 for i, r in enumerate(out_rows)
                  if all(not v.strip() for v in r.values())]
    if not empty_rows:
        report("pass", "No Empty Rows", "All rows have data")
    else:
        report("fail", "No Empty Rows", f"{len(empty_rows)} empty rows",
            examples=[f"Row {r}" for r in empty_rows])

    # If input file provided, do cross-file validation
    if args.input:
        try:
            with open(args.input, "r", encoding="utf-8-sig", newline="") as f:
                in_reader = csv.DictReader(f)
                in_headers = list(in_reader.fieldnames or [])
                in_rows = list(in_reader)
        except UnicodeDecodeError:
            with open(args.input, "r", encoding="latin-1", newline="") as f:
                in_reader = csv.DictReader(f)
                in_headers = list(in_reader.fieldnames or [])
                in_rows = list(in_reader)

        # File-swap detection
        if set(in_headers) & {"Elector No. Prefix", "Full Elector No."}:
            report("warn", "Input Format",
                "Input has TTW headers -- possible file swap?")

        # Row count check
        deleted = 0
        if args.report:
            try:
                _, machine = read_report(args.report)
                deleted = sum(1 for l in machine if l.startswith("DELETED"))
            except Exception:
                pass

        expected_out = len(in_rows) - deleted
        if len(out_rows) == expected_out:
            report("pass", "Row Count",
                f"{len(in_rows)} in, {len(out_rows)} out, {deleted} deleted (all accounted for)")
        else:
            report("fail", "Row Count",
                f"{len(in_rows)} in, {len(out_rows)} out, {deleted} deleted "
                f"(expected {expected_out} out)")

        # Name preservation check (lowercase comparison — names may be case-normalized)
        in_names = set()
        for r in in_rows:
            fn = r.get("ElectorForename", "").strip()
            sn = r.get("ElectorSurname", "").strip()
            if fn or sn:
                in_names.add((fn.lower(), sn.lower()))

        out_names = set()
        for r in out_rows:
            fn = r.get("Forename", "").strip()
            sn = r.get("Surname", "").strip()
            if fn or sn:
                out_names.add((fn.lower(), sn.lower()))

        # Names in input should appear in output (minus deleted)
        name_diff = in_names - out_names
        if len(name_diff) <= deleted:
            report("pass", "Name Preservation",
                f"All {len(out_names)} output names traceable to input")
        else:
            report("warn", "Name Preservation",
                f"{len(name_diff)} input names not found in output (expected {deleted})",
                examples=[f"{fn} {sn}" for fn, sn in sorted(name_diff)])

        # PD completeness
        in_pd_counts = Counter(r.get("PDCode", "").strip() for r in in_rows)
        out_pd_counts = Counter(r.get("Elector No. Prefix", "").strip() for r in out_rows)
        pd_issues = []
        for pd, in_count in in_pd_counts.items():
            out_count = out_pd_counts.get(pd, 0)
            if out_count > in_count:
                pd_issues.append(f"{pd}: {in_count} in, {out_count} out")
        if not pd_issues:
            report("pass", "PD Completeness",
                f"All {len(in_pd_counts)} polling districts accounted for")
        else:
            report("fail", "PD Completeness", f"{len(pd_issues)} polling districts with issues",
                examples=pd_issues)

    # Summary
    print()
    print("-" * 60)
    total = results["pass"] + results["fail"] + results["warn"]
    print(f"Results: {results['pass']}/{total} passed, "
          f"{results['fail']} failed, {results['warn']} warnings")

    return results


# ---------------------------------------------------------------------------
# Column Alias Tests
# ---------------------------------------------------------------------------

class TestColumnAliases(unittest.TestCase):
    """Test automatic column name alias resolution."""

    # Minimal council-format CSV using TTW-style names (Address1 instead of RegisteredAddress1)
    TTW_STYLE_HEADERS = "Address1,Forename,Surname,PostCode,PDCode,RollNo"
    TTW_STYLE_ROW = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"

    def _write_csv(self, header_line, data_lines):
        """Write a temp CSV with given headers and data lines. Returns path."""
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w",
                                          encoding="utf-8", newline="")
        tmp.write(header_line + "\n")
        if isinstance(data_lines, str):
            data_lines = [data_lines]
        for line in data_lines:
            tmp.write(line + "\n")
        tmp.close()
        return tmp.name

    def test_alias_address1_to_registered_address1(self):
        """Input with 'Address1' (no 'RegisteredAddress1') maps to RegisteredAddress1."""
        inp = self._write_csv(self.TTW_STYLE_HEADERS, self.TTW_STYLE_ROW)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            # TTW output should have Address1 (the TTW output name from FIELD_MAP)
            self.assertIn("Address1", headers)
            self.assertEqual(rows[0]["Address1"], "42 Chamberlayne Rd")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_forename_to_elector_forename(self):
        """Input with 'Forename' (no 'ElectorForename') maps correctly."""
        inp = self._write_csv(self.TTW_STYLE_HEADERS, self.TTW_STYLE_ROW)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("Forename", headers)
            self.assertEqual(rows[0]["Forename"], "John")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_case_insensitive(self):
        """address1, ADDRESS1, Address1 all resolve to RegisteredAddress1."""
        for variant in ["address1", "ADDRESS1", "Address1"]:
            header = f"{variant},Forename,Surname,PostCode,PDCode,RollNo"
            inp = self._write_csv(header, self.TTW_STYLE_ROW)
            tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
            tmp_out.close()
            try:
                rc, _, stderr = run_clean(inp, tmp_out.name)
                self.assertEqual(rc, 0, f"Failed with '{variant}':\n{stderr}")
                headers, rows = read_output_csv(tmp_out.name)
                self.assertIn("Address1", headers,
                              f"Address1 missing in output with input '{variant}'")
            finally:
                os.unlink(inp)
                os.unlink(tmp_out.name)

    def test_alias_with_spaces(self):
        """'Post Code', 'First Name', 'Address 1' all resolve."""
        header = "Address 1,First Name,Last Name,Post Code,PD Code,Roll No"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("PostCode", headers)
            self.assertEqual(rows[0]["PostCode"], "NW10 3JU")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_with_underscores(self):
        """'Post_Code', 'First_Name', 'Registered_Address_1' all resolve."""
        header = "Registered_Address_1,First_Name,Last_Name,Post_Code,PD_Code,Roll_No"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("PostCode", headers)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_council_name_wins(self):
        """If both RegisteredAddress1 and Address1 exist, RegisteredAddress1 is used."""
        header = "RegisteredAddress1,Address1,ElectorForename,ElectorSurname,PostCode,PDCode,RollNo"
        row = "Council Addr,TTW Addr,John,Smith,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            # Address1 in output should come from RegisteredAddress1 (the council name)
            self.assertEqual(rows[0]["Address1"], "Council Addr")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_log_in_report(self):
        """Alias resolutions appear in QA report."""
        inp = self._write_csv(self.TTW_STYLE_HEADERS, self.TTW_STYLE_ROW)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()
        try:
            rc, _, _ = run_clean(inp, tmp_out.name, report_file=tmp_report.name)
            self.assertEqual(rc, 0)
            report_text = Path(tmp_report.name).read_text()
            self.assertIn("Column Aliases Resolved", report_text)
            # "Address1" should be logged as resolved to "RegisteredAddress1"
            self.assertIn("'Address1' -> 'RegisteredAddress1'", report_text)
            self.assertIn("'Forename' -> 'ElectorForename'", report_text)
            self.assertIn("'Surname' -> 'ElectorSurname'", report_text)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)

    def test_alias_log_stderr(self):
        """Alias resolutions printed to stderr when not --quiet."""
        inp = self._write_csv(self.TTW_STYLE_HEADERS, self.TTW_STYLE_ROW)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            # Run WITHOUT --quiet (override run_clean's default)
            cmd = [sys.executable, str(TOOL), inp, tmp_out.name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, f"Failed:\n{result.stderr}")
            self.assertIn("Resolved", result.stderr)
            self.assertIn("'Address1' -> 'RegisteredAddress1'", result.stderr)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_no_aliases_flag(self):
        """--no-aliases disables alias resolution, causing validation failure."""
        inp = self._write_csv(self.TTW_STYLE_HEADERS, self.TTW_STYLE_ROW)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name, extra_args=["--no-aliases"])
            # Should fail because 'Address1' is not 'RegisteredAddress1'
            self.assertNotEqual(rc, 0, "Should fail without alias resolution")
            self.assertIn("Missing required columns", stderr)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_full_ttw_input(self):
        """Input using all TTW-style names passes validation after alias resolution."""
        header = ("Address1,Address2,Address3,Address4,Address5,Address6,"
                  "Forename,Middle Names,Surname,PostCode,PDCode,RollNo,"
                  "Date of Attainment,UPRN,Suffix")
        row = ("42 Chamberlayne Rd,London,,,,,"
               "John,,Smith,NW10 3JU,KG1,1,"
               "01/01/2000,123456789,")
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            # All core TTW output fields should be present
            self.assertIn("Elector No. Prefix", headers)
            self.assertIn("Elector No.", headers)
            self.assertIn("Address1", headers)
            self.assertIn("Forename", headers)
            self.assertIn("PostCode", headers)
            self.assertEqual(rows[0]["Forename"], "John")
            self.assertEqual(rows[0]["Surname"], "Smith")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_existing_golden_input_unaffected(self):
        """Golden input with exact council names should not trigger any alias renames."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()
        try:
            rc, _, stderr = run_clean(
                TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
                report_file=tmp_report.name,
            )
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            report_text = Path(tmp_report.name).read_text()
            # No alias resolution should appear in the report
            self.assertNotIn("Column Aliases Resolved", report_text)
        finally:
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)

    def test_alias_with_hyphens(self):
        """Hyphenated column names: 'Post-Code', 'First-Name', 'Registered-Address-1'."""
        header = "Registered-Address-1,First-Name,Last-Name,Post-Code,PD-Code,Roll-No"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("PostCode", headers)
            self.assertIn("Address1", headers)
            self.assertEqual(rows[0]["PostCode"], "NW10 3JU")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_with_dots(self):
        """Dot-containing column names are normalized (dots stripped during matching)."""
        # Use non-TTW-indicator names with dots to test dot stripping
        header = "Reg.Address.1,Elector.Forename,Elector.Surname,Post.Code,P.D.Code,Roll.No"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("Address1", headers)
            self.assertIn("PostCode", headers)
            self.assertEqual(rows[0]["PostCode"], "NW10 3JU")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_address2_through_6(self):
        """TTW-style Address2-Address6 all resolve to RegisteredAddress2-6."""
        header = "Address1,Address2,Address3,Address4,Address5,Address6,Forename,Surname,PostCode,PDCode,RollNo"
        row = "42 Chamberlayne Rd,London,Brent,,,,,John,Smith,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        tmp_report = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_report.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name, report_file=tmp_report.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            report_text = Path(tmp_report.name).read_text()
            # Verify all Address aliases were resolved
            self.assertIn("'Address1' -> 'RegisteredAddress1'", report_text)
            self.assertIn("'Address2' -> 'RegisteredAddress2'", report_text)
            self.assertIn("'Address3' -> 'RegisteredAddress3'", report_text)
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("Address1", headers)
            self.assertIn("Address2", headers)
            self.assertEqual(rows[0]["Address1"], "42 Chamberlayne Rd")
            self.assertEqual(rows[0]["Address2"], "London")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)
            os.unlink(tmp_report.name)

    def test_alias_informal_canvasser_names(self):
        """Informal names: 'Given Name', 'Family Name', 'Polling District', 'Roll Number'."""
        header = "Given Name,Family Name,Polling District,Roll Number,Address1,Post Code"
        row = "John,Smith,KG1,1,42 Chamberlayne Rd,NW10 3JU"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("Forename", headers)
            self.assertIn("Surname", headers)
            self.assertEqual(rows[0]["Forename"], "John")
            self.assertEqual(rows[0]["Surname"], "Smith")
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_postcode_one_word(self):
        """'Postcode' (one word, various casings) resolves to PostCode."""
        for variant in ["Postcode", "postcode", "POSTCODE"]:
            header = f"Address1,Forename,Surname,{variant},PDCode,RollNo"
            row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
            inp = self._write_csv(header, row)
            tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
            tmp_out.close()
            try:
                rc, _, stderr = run_clean(inp, tmp_out.name)
                self.assertEqual(rc, 0, f"Failed with '{variant}':\n{stderr}")
                headers, rows = read_output_csv(tmp_out.name)
                self.assertIn("PostCode", headers,
                              f"PostCode missing with input '{variant}'")
                self.assertEqual(rows[0]["PostCode"], "NW10 3JU")
            finally:
                os.unlink(inp)
                os.unlink(tmp_out.name)

    def test_alias_leading_trailing_whitespace(self):
        """Column names with leading/trailing whitespace resolve correctly."""
        header = " Address1 , Forename , Surname , PostCode , PDCode , RollNo "
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            # Should have resolved the whitespace-padded names
            self.assertIn("PostCode", headers)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_duplicate_collision_keeps_first(self):
        """When two aliases map to same canonical, first one wins, second passes through."""
        header = "Forename,First Name,Surname,Address1,PostCode,PDCode,RollNo"
        row = "John,Jonathan,Smith,42 Chamberlayne Rd,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            # "Forename" maps to ElectorForename (and gets output as "Forename")
            self.assertIn("Forename", headers)
            self.assertEqual(rows[0]["Forename"], "John")
            # "First Name" should pass through as extra since ElectorForename is taken
            self.assertIn("First Name", headers)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_duplicate_collision_stderr_warning(self):
        """Duplicate alias collision prints a warning to stderr."""
        header = "Forename,First Name,Surname,Address1,PostCode,PDCode,RollNo"
        row = "John,Jonathan,Smith,42 Chamberlayne Rd,NW10 3JU,KG1,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            # Run WITHOUT --quiet to see stderr warnings
            cmd = [sys.executable, str(TOOL), inp, tmp_out.name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, f"Failed:\n{result.stderr}")
            self.assertIn("also maps to", result.stderr)
            self.assertIn("First Name", result.stderr)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_mixed_separators(self):
        """Mixed separators within a column name: 'Registered_Address 1', 'Elector-No Prefix'."""
        header = "Registered_Address 1,Elector-No Prefix,Post_Code,Elector Forename,Elector_Surname,Roll_No"
        row = "42 Chamberlayne Rd,KG1,NW10 3JU,John,Smith,1"
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn("PostCode", headers)
            self.assertIn("Forename", headers)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)


# ---------------------------------------------------------------------------
# Unit Tests: _norm_col() and resolve_aliases()
# ---------------------------------------------------------------------------

class TestNormCol(unittest.TestCase):
    """Unit tests for _norm_col() normalization function."""

    def test_lowercase(self):
        self.assertEqual(_norm_col("PostCode"), "postcode")

    def test_strip_spaces(self):
        self.assertEqual(_norm_col("Post Code"), "postcode")

    def test_strip_underscores(self):
        self.assertEqual(_norm_col("Post_Code"), "postcode")

    def test_strip_hyphens(self):
        self.assertEqual(_norm_col("Post-Code"), "postcode")

    def test_strip_dots(self):
        self.assertEqual(_norm_col("Elector No."), "electorno")
        self.assertEqual(_norm_col("Elector No. Prefix"), "electornoprefix")

    def test_strip_mixed_separators(self):
        self.assertEqual(_norm_col("Registered_Address 1"), "registeredaddress1")
        self.assertEqual(_norm_col("Elector-No. Prefix"), "electornoprefix")

    def test_leading_trailing_whitespace(self):
        self.assertEqual(_norm_col("  PostCode  "), "postcode")
        self.assertEqual(_norm_col(" Address1 "), "address1")

    def test_empty_string(self):
        self.assertEqual(_norm_col(""), "")

    def test_only_separators(self):
        """Strings of only dots/spaces/underscores/hyphens normalize to empty."""
        self.assertEqual(_norm_col("..."), "")
        self.assertEqual(_norm_col("___"), "")
        self.assertEqual(_norm_col("   "), "")
        self.assertEqual(_norm_col("-.-_"), "")

    def test_all_uppercase(self):
        self.assertEqual(_norm_col("POSTCODE"), "postcode")

    def test_consecutive_separators(self):
        self.assertEqual(_norm_col("Post__Code"), "postcode")
        self.assertEqual(_norm_col("Post - Code"), "postcode")


class TestResolveAliasesUnit(unittest.TestCase):
    """Unit tests for resolve_aliases() function."""

    def test_no_aliases_needed(self):
        """Headers that are already canonical produce no renames."""
        headers = ["PDCode", "RollNo", "ElectorForename", "ElectorSurname",
                    "RegisteredAddress1", "PostCode"]
        new_headers, log = resolve_aliases(headers, quiet=True)
        self.assertEqual(new_headers, headers)
        self.assertEqual(log, [])

    def test_basic_rename(self):
        """TTW-style names get renamed to canonical."""
        headers = ["Address1", "Forename", "Surname", "PostCode", "PDCode", "RollNo"]
        new_headers, log = resolve_aliases(headers, quiet=True)
        self.assertIn("RegisteredAddress1", new_headers)
        self.assertIn("ElectorForename", new_headers)
        self.assertIn("ElectorSurname", new_headers)
        self.assertEqual(len(log), 3)  # Address1, Forename, Surname

    def test_canonical_takes_precedence(self):
        """If canonical name already present, alias is not renamed."""
        headers = ["RegisteredAddress1", "Address1", "ElectorForename", "ElectorSurname",
                    "PostCode", "PDCode", "RollNo"]
        new_headers, log = resolve_aliases(headers, quiet=True)
        # Address1 should NOT be renamed because RegisteredAddress1 already present
        self.assertEqual(new_headers[0], "RegisteredAddress1")
        self.assertEqual(new_headers[1], "Address1")
        rename_targets = [canon for _, canon in log]
        self.assertNotIn("RegisteredAddress1", rename_targets)

    def test_duplicate_alias_not_renamed(self):
        """Second alias for same canonical passes through unrenamed."""
        headers = ["Forename", "First Name", "Surname", "Address1", "PostCode",
                    "PDCode", "RollNo"]
        new_headers, log = resolve_aliases(headers, quiet=True)
        # Forename -> ElectorForename (renamed)
        self.assertEqual(new_headers[0], "ElectorForename")
        # First Name stays as-is (ElectorForename already claimed)
        self.assertEqual(new_headers[1], "First Name")

    def test_case_insensitive_matching(self):
        headers = ["address1", "FORENAME", "Surname", "postcode", "pdcode", "rollno"]
        new_headers, log = resolve_aliases(headers, quiet=True)
        self.assertIn("RegisteredAddress1", new_headers)
        self.assertIn("ElectorForename", new_headers)
        self.assertIn("PostCode", new_headers)
        self.assertIn("PDCode", new_headers)


# ---------------------------------------------------------------------------
# Tests for Untested Alias Entries
# ---------------------------------------------------------------------------

class TestAliasEntries(unittest.TestCase):
    """Verify specific alias entries that weren't covered by TestColumnAliases."""

    TTW_STYLE_ROW = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
    BASE_HEADER_SUFFIX = ",Forename,Surname,PostCode,PDCode,RollNo"

    def _write_csv(self, header_line, data_lines):
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w",
                                          encoding="utf-8", newline="")
        tmp.write(header_line + "\n")
        if isinstance(data_lines, str):
            data_lines = [data_lines]
        for line in data_lines:
            tmp.write(line + "\n")
        tmp.close()
        return tmp.name

    def _run_and_check(self, header, row, expected_output_col, expected_value=None):
        """Run clean_register and verify a specific output column exists."""
        inp = self._write_csv(header, row)
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(inp, tmp_out.name)
            self.assertEqual(rc, 0, f"Failed:\n{stderr}")
            headers, rows = read_output_csv(tmp_out.name)
            self.assertIn(expected_output_col, headers,
                          f"Expected '{expected_output_col}' in output headers")
            if expected_value is not None:
                self.assertEqual(rows[0][expected_output_col], expected_value)
        finally:
            os.unlink(inp)
            os.unlink(tmp_out.name)

    def test_alias_doa_to_date_of_attainment(self):
        """'DOA' resolves to DateOfAttainment."""
        header = "Address1,Forename,Surname,PostCode,PDCode,RollNo,DOA"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1,01/01/2000"
        self._run_and_check(header, row, "Date of Attainment", "01/01/2000")

    def test_alias_dateattained_to_date_of_attainment(self):
        """'Date Attained' resolves to DateOfAttainment."""
        header = "Address1,Forename,Surname,PostCode,PDCode,RollNo,Date Attained"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1,15/06/2005"
        self._run_and_check(header, row, "Date of Attainment", "15/06/2005")

    def test_alias_attainmentdate_to_date_of_attainment(self):
        """'Attainment Date' resolves to DateOfAttainment."""
        header = "Address1,Forename,Surname,PostCode,PDCode,RollNo,Attainment Date"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1,25/12/2010"
        self._run_and_check(header, row, "Date of Attainment", "25/12/2010")

    def test_alias_middlename_to_elector_middlename(self):
        """'Middle Name' resolves to ElectorMiddleName."""
        header = "Address1,Forename,Surname,PostCode,PDCode,RollNo,Middle Name"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1,William"
        self._run_and_check(header, row, "Middle Names", "William")

    def test_alias_middlenames_to_elector_middlename(self):
        """'Middle Names' resolves to ElectorMiddleName."""
        header = "Address1,Forename,Surname,PostCode,PDCode,RollNo,Middle Names"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1,William James"
        self._run_and_check(header, row, "Middle Names", "William James")

    def test_alias_electorid(self):
        """'Elector ID' resolves to ElectorID (council-only column, preserved)."""
        header = "Address1,Forename,Surname,PostCode,PDCode,RollNo,Elector ID"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1,E12345"
        self._run_and_check(header, row, "ElectorID", "E12345")

    def test_alias_zipcode_to_postcode(self):
        """'Zip Code' resolves to PostCode."""
        header = "Address1,Forename,Surname,Zip Code,PDCode,RollNo"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        self._run_and_check(header, row, "PostCode", "NW10 3JU")

    def test_alias_regaddress1(self):
        """'Reg Address 1' resolves to RegisteredAddress1."""
        header = "Reg Address 1,Forename,Surname,PostCode,PDCode,RollNo"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        self._run_and_check(header, row, "Address1", "42 Chamberlayne Rd")

    def test_alias_regaddress2(self):
        """'Reg. Address 2' resolves to RegisteredAddress2."""
        header = "Address1,Reg. Address 2,Forename,Surname,PostCode,PDCode,RollNo"
        row = "42 Chamberlayne Rd,London,John,Smith,NW10 3JU,KG1,1"
        self._run_and_check(header, row, "Address2", "London")

    def test_alias_electornumber(self):
        """'Elector Number' resolves to RollNo."""
        header = "Address1,Forename,Surname,PostCode,PDCode,Elector Number"
        row = "42 Chamberlayne Rd,John,Smith,NW10 3JU,KG1,1"
        self._run_and_check(header, row, "Elector No.", "1")

    def test_file_swap_detected_before_aliases(self):
        """TTW-format file is rejected even when aliases would resolve the headers."""
        header = "Elector No. Prefix,Elector No.,Elector No. Suffix,Full Elector No.,Forename,Surname,Address1,PostCode"
        row = "KG1,1,0,KG1-1-0,John,Smith,42 Chamberlayne Rd,NW10 3JU"
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w",
                                          encoding="utf-8", newline="")
        tmp.write(header + "\n")
        tmp.write(row + "\n")
        tmp.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(tmp.name, tmp_out.name)
            self.assertNotEqual(rc, 0, "Should reject TTW-format input")
            self.assertIn("TTW format", stderr)
        finally:
            os.unlink(tmp.name)
            os.unlink(tmp_out.name)

    def test_partial_ttw_file_detected(self):
        """Partial TTW file with 'Elector No. Suffix' (no 'Full Elector No.') is still caught."""
        header = "Elector No. Suffix,Forename,Surname,Address1,PostCode,PDCode,RollNo"
        row = "0,John,Smith,42 Chamberlayne Rd,NW10 3JU,KG1,1"
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w",
                                          encoding="utf-8", newline="")
        tmp.write(header + "\n")
        tmp.write(row + "\n")
        tmp.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        try:
            rc, _, stderr = run_clean(tmp.name, tmp_out.name)
            self.assertNotEqual(rc, 0, "Should reject file with TTW indicator header")
            self.assertIn("TTW format", stderr)
        finally:
            os.unlink(tmp.name)
            os.unlink(tmp_out.name)


# ---------------------------------------------------------------------------
# Pad-reference tests
# ---------------------------------------------------------------------------

def _write_temp_csv(rows, headers, encoding="utf-8-sig"):
    """Write rows to a temp CSV and return the path."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


_PAD_COUNCIL_HEADERS = [
    "PDCode", "RollNo", "ElectorSurname", "ElectorForename",
    "RegisteredAddress1", "RegisteredAddress2",
    "RegisteredAddress3", "RegisteredAddress4",
    "RegisteredAddress5", "RegisteredAddress6",
    "PostCode", "SubHouse", "House",
]

_PAD_TTW_HEADERS = [
    "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
    "Surname", "Forename", "Address1", "Address2", "PostCode",
]


def _make_council_row(pdcode, rollno, surname, forename, addr1, addr2, postcode,
                       sub_house="", house="",
                       addr3="", addr4="", addr5="", addr6=""):
    base = {h: "" for h in _PAD_COUNCIL_HEADERS}
    base.update({"PDCode": pdcode, "RollNo": rollno, "ElectorSurname": surname,
                 "ElectorForename": forename, "RegisteredAddress1": addr1,
                 "RegisteredAddress2": addr2,
                 "RegisteredAddress3": addr3, "RegisteredAddress4": addr4,
                 "RegisteredAddress5": addr5, "RegisteredAddress6": addr6,
                 "PostCode": postcode, "SubHouse": sub_house, "House": house})
    return base


def _make_ttw_row(addr1, addr2, postcode):
    base = {h: "" for h in _PAD_TTW_HEADERS}
    base.update({"Address1": addr1, "Address2": addr2, "PostCode": postcode,
                 "Elector No. Prefix": "KG1", "Elector No.": "1",
                 "Elector No. Suffix": "0", "Full Elector No.": "KG1-1-0",
                 "Surname": "Test", "Forename": "Test"})
    return base


class TestPadReference(unittest.TestCase):
    """Tests for --full-register flag on zero-padding."""

    def _run_with_reference(self, update_rows, reference_rows):
        update_path = _write_temp_csv(update_rows, _PAD_COUNCIL_HEADERS)
        ref_path = _write_temp_csv(reference_rows, _PAD_TTW_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      extra_args=["--full-register", ref_path])
            self.assertEqual(rc, 0, stderr)
            return read_output_csv(out_path)
        finally:
            for p in [update_path, ref_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def _run_without_reference(self, update_rows):
        update_path = _write_temp_csv(update_rows, _PAD_COUNCIL_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_clean(update_path, out_path)
            self.assertEqual(rc, 0, stderr)
            return read_output_csv(out_path)
        finally:
            for p in [update_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_flat_padded_with_reference(self):
        """Single Flat 5 in update. Reference has Flat 1-20. Should pad to Flat 05."""
        update = [_make_council_row("KG1", "1", "Test", "A", "Flat 5", "30 High Road", "NW10 3JU")]
        ref = [_make_ttw_row(f"Flat {i}", "30 High Road", "NW10 3JU") for i in range(1, 21)]
        _, rows = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Address1"], "Flat 05")

    def test_flat_not_padded_without_reference(self):
        """Single Flat 5 without reference. Width=1, should NOT pad."""
        update = [_make_council_row("KG1", "1", "Test", "A", "Flat 5", "30 High Road", "NW10 3JU")]
        _, rows = self._run_without_reference(update)
        self.assertEqual(rows[0]["Address1"], "Flat 5")

    def test_building_padded_with_reference(self):
        """Single '3 Sheil Court' in update. Reference has Sheil Court 1-15. Should pad to 03."""
        update = [_make_council_row("KG1", "1", "Test", "A", "3 Sheil Court", "30 High Road", "NW10 3JU")]
        ref = [_make_ttw_row(f"Sheil Court {i}", "30 High Road", "NW10 3JU") for i in range(1, 16)]
        _, rows = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Address1"], "Sheil Court 03")

    def test_unknown_building_falls_back(self):
        """Update has a building not in reference. Falls back to own data."""
        update = [_make_council_row("KG1", "1", "Test", "A", "Flat 5", "99 Unknown Road", "NW10 9ZZ")]
        ref = [_make_ttw_row(f"Flat {i}", "30 High Road", "NW10 3JU") for i in range(1, 21)]
        _, rows = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Address1"], "Flat 5")

    def test_wider_in_update_uses_wider(self):
        """Reference max width 1. Update has Flat 100. Should use width 3."""
        update = [
            _make_council_row("KG1", "1", "Test", "A", "Flat 5", "30 High Road", "NW10 3JU"),
            _make_council_row("KG1", "2", "Test", "B", "Flat 100", "30 High Road", "NW10 3JU"),
        ]
        ref = [_make_ttw_row(f"Flat {i}", "30 High Road", "NW10 3JU") for i in range(1, 10)]
        _, rows = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Address1"], "Flat 005")
        self.assertEqual(rows[1]["Address1"], "Flat 100")

    def test_letter_suffix_with_reference(self):
        """Reference has Flat 1A-12B. Update has Flat 5. Width from numeric part -> pad to 2."""
        ref = [_make_ttw_row(f"Flat {i}A", "30 High Road", "NW10 3JU") for i in range(1, 13)]
        update = [_make_council_row("KG1", "1", "Test", "A", "Flat 5", "30 High Road", "NW10 3JU")]
        _, rows = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Address1"], "Flat 05")


class TestSuffixReference(unittest.TestCase):
    """Tests for suffix-aware --full-register: decimal RollNos skip existing suffixes."""

    def _make_ref_row(self, prefix, number, suffix):
        """Create a TTW-format reference row with specific elector number parts."""
        row = _make_ttw_row("Flat 1", "30 High Road", "NW10 3JU")
        row["Elector No. Prefix"] = prefix
        row["Elector No."] = number
        row["Elector No. Suffix"] = suffix
        fen = f"{prefix}-{number}-{suffix}" if suffix else f"{prefix}-{number}"
        row["Full Elector No."] = fen
        return row

    def _run_with_reference(self, update_rows, reference_rows):
        update_path = _write_temp_csv(update_rows, _PAD_COUNCIL_HEADERS)
        ref_path = _write_temp_csv(reference_rows, _PAD_TTW_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        fd2, report_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd2)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      extra_args=["--full-register", ref_path],
                                      report_file=report_path)
            self.assertEqual(rc, 0, stderr)
            headers, rows = read_output_csv(out_path)
            report_text = Path(report_path).read_text(encoding="utf-8")
            return headers, rows, report_text
        finally:
            for p in [update_path, ref_path, out_path, report_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def _run_without_reference(self, update_rows):
        update_path = _write_temp_csv(update_rows, _PAD_COUNCIL_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_clean(update_path, out_path)
            self.assertEqual(rc, 0, stderr)
            return read_output_csv(out_path)
        finally:
            for p in [update_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_decimal_suffixes_skip_existing(self):
        """Reference has KG1-1416 with suffix 0. Update has 1416.5 and 1416.75.
        Should get suffixes 1 and 2 (skipping 0)."""
        ref = [self._make_ref_row("KG1", "1416", "0")]
        update = [
            _make_council_row("KG1", "1416.5", "Test", "A", "Flat 1", "High Road", "NW10 3JU"),
            _make_council_row("KG1", "1416.75", "Test", "B", "Flat 2", "High Road", "NW10 3JU"),
        ]
        _, rows, _ = self._run_with_reference(update, ref)
        suffixes = [r["Elector No. Suffix"] for r in rows]
        self.assertEqual(suffixes, ["1", "2"])

    def test_decimal_suffixes_without_reference(self):
        """Without reference, 1416.5 and 1416.75 get suffixes 0 and 1."""
        update = [
            _make_council_row("KG1", "1416.5", "Test", "A", "Flat 1", "High Road", "NW10 3JU"),
            _make_council_row("KG1", "1416.75", "Test", "B", "Flat 2", "High Road", "NW10 3JU"),
        ]
        _, rows = self._run_without_reference(update)
        suffixes = [r["Elector No. Suffix"] for r in rows]
        self.assertEqual(suffixes, ["0", "1"])

    def test_decimal_suffixes_skip_multiple_existing(self):
        """Reference has suffixes 0 and 1. Update decimal should get suffix 2."""
        ref = [
            self._make_ref_row("KG1", "100", "0"),
            self._make_ref_row("KG1", "100", "1"),
        ]
        update = [
            _make_council_row("KG1", "100.5", "Test", "A", "Flat 1", "High Road", "NW10 3JU"),
        ]
        _, rows, _ = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "2")

    def test_different_prefix_no_clash(self):
        """Reference has KG1-100 suffix 0. Update has HP1-100.5. Different prefix = no clash."""
        ref = [self._make_ref_row("KG1", "100", "0")]
        update = [
            _make_council_row("HP1", "100.5", "Test", "A", "Flat 1", "High Road", "NW10 3JU"),
        ]
        _, rows, _ = self._run_with_reference(update, ref)
        # HP1-100 has no reference entry, so suffix starts at 0
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_full_elector_no_built_correctly(self):
        """Verify Full Elector No. uses the correct suffix after reference skip."""
        ref = [self._make_ref_row("KG1", "50", "0")]
        update = [
            _make_council_row("KG1", "50.5", "Test", "A", "Flat 1", "High Road", "NW10 3JU"),
        ]
        _, rows, _ = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Full Elector No."], "KG1-50-1")

    def test_clash_detection_in_report(self):
        """If a clash would have occurred, suffix is reassigned and logged."""
        ref = [self._make_ref_row("KG1", "200", "0")]
        update = [
            _make_council_row("KG1", "200", "Test", "A", "Flat 1", "High Road", "NW10 3JU"),
        ]
        _, rows, report = self._run_with_reference(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")
        self.assertEqual(rows[0]["Full Elector No."], "KG1-200-1")
        self.assertIn("clash", report.lower())

    def test_multi_row_clash_each_gets_distinct_suffix(self):
        """Two non-decimal update rows both default to suffix 0, reference has 0.
        Each must get a distinct suffix (1 and 2), and the CORRECT row is modified."""
        ref = [self._make_ref_row("KG1", "300", "0")]
        # Note: _make_council_row args are (pdcode, rollno, SURNAME, FORENAME, ...)
        update = [
            _make_council_row("KG1", "300", "Smith", "Alice", "Flat 1", "High Road", "NW10 3JU"),
            _make_council_row("KG1", "300", "Jones", "Bob", "Flat 2", "High Road", "NW10 3JU"),
        ]
        _, rows, _ = self._run_with_reference(update, ref)
        self.assertEqual(len(rows), 2)
        # Both rows should have distinct suffixes, neither should be "0"
        suffixes = {r["Elector No. Suffix"] for r in rows}
        self.assertNotIn("0", suffixes, "Suffix 0 clashes with reference")
        self.assertEqual(len(suffixes), 2, "Each row must have a distinct suffix")
        # Verify correct row has correct name (shadowing bug would modify wrong row)
        alice = [r for r in rows if r["Forename"] == "Alice"][0]
        bob = [r for r in rows if r["Forename"] == "Bob"][0]
        self.assertNotEqual(alice["Elector No. Suffix"], bob["Elector No. Suffix"])
        self.assertIn(alice["Elector No. Suffix"], alice["Full Elector No."])
        self.assertIn(bob["Elector No. Suffix"], bob["Full Elector No."])


# ---------------------------------------------------------------------------
# TTW app-export reference tests
# ---------------------------------------------------------------------------

# Minimal TTW app-export headers (what build_padding_reference reads)
_APP_EXPORT_REF_HEADERS = [
    "Voter Number", "First Name", "Middle Name", "Surname",
    "House Name", "House Number", "Road", "Post Code",
]


def _make_app_ref_row(voter_number, first_name="Test", surname="Test",
                       house_name="", house_number="", road="High Road",
                       post_code="NW10 3JU"):
    base = {h: "" for h in _APP_EXPORT_REF_HEADERS}
    base.update({
        "Voter Number": voter_number,
        "First Name": first_name,
        "Surname": surname,
        "House Name": house_name,
        "House Number": house_number,
        "Road": road,
        "Post Code": post_code,
    })
    return base


class TestAppExportReference(unittest.TestCase):
    """Tests for --full-register with TTW app-export CSV format."""

    def _run_with_app_ref(self, update_rows, reference_rows, update_headers=None):
        headers = update_headers or _PAD_COUNCIL_HEADERS
        update_path = _write_temp_csv(update_rows, headers)
        ref_path = _write_temp_csv(reference_rows, _APP_EXPORT_REF_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        fd2, report_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd2)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      extra_args=["--full-register", ref_path],
                                      report_file=report_path)
            self.assertEqual(rc, 0, stderr)
            headers_out, rows = read_output_csv(out_path)
            report_text = Path(report_path).read_text(encoding="utf-8")
            return headers_out, rows, report_text
        finally:
            for p in [update_path, ref_path, out_path, report_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_flat_padded_from_app_export_ref(self):
        """App-export ref has Flat 1-20. Single update Flat 5 pads to Flat 05."""
        ref = [_make_app_ref_row(f"KG1-{i}-0", house_name=f"Flat {i}",
                                  road="30 High Road")
               for i in range(1, 21)]
        update = [_make_council_row("KG1", "99", "New", "Person",
                                     "Flat 5", "30 High Road", "NW10 3JU")]
        _, rows, _ = self._run_with_app_ref(update, ref)
        self.assertEqual(rows[0]["Address1"], "Flat 05")

    def test_building_padded_from_app_export_ref(self):
        """App-export ref has Sheil Court 1-15. Single update pads to 03."""
        ref = [_make_app_ref_row(f"KG1-{i}-0", house_name="Sheil Court",
                                  house_number=str(i), road="High Road")
               for i in range(1, 16)]
        update = [_make_council_row("KG1", "99", "New", "Person",
                                     "3 Sheil Court", "High Road", "NW10 3JU")]
        _, rows, _ = self._run_with_app_ref(update, ref)
        self.assertEqual(rows[0]["Address1"], "Sheil Court 03")

    def test_suffix_from_app_export_voter_number(self):
        """A/D row matched to app-export ref by Voter Number suffix."""
        ref = [_make_app_ref_row("KG1-100-0", first_name="John", surname="Smith",
                                  house_name="Flat 1", road="High Road")]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run_with_app_ref(update, ref,
                                             update_headers=_PAD_COUNCIL_HEADERS_CT)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_suffix_multi_candidate_from_app_export(self):
        """Two app-export entries with same prefix-number, A row picks correct one by name."""
        ref = [
            _make_app_ref_row("KG1-100-0", first_name="John", surname="Smith",
                               house_name="Flat 1", road="High Road"),
            _make_app_ref_row("KG1-100-1", first_name="Jane", surname="Smith",
                               house_name="Flat 1", road="High Road"),
        ]
        update = [_make_council_row_ct("KG1", "100", "Smith", "Jane",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run_with_app_ref(update, ref,
                                             update_headers=_PAD_COUNCIL_HEADERS_CT)
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")

    def test_n_row_skips_app_export_suffixes(self):
        """N row avoids suffix already taken in app-export ref."""
        ref = [_make_app_ref_row("KG1-100-0", first_name="John", surname="Smith")]
        update = [_make_council_row_ct("KG1", "100.5", "New", "Person",
                                        "Flat 3", "High Road", "NW10 3JU", "N")]
        _, rows, _ = self._run_with_app_ref(update, ref,
                                             update_headers=_PAD_COUNCIL_HEADERS_CT)
        self.assertNotEqual(rows[0]["Elector No. Suffix"], "0")

    def test_dummy_voter_number_excluded(self):
        """Voter Number '--0' (dummy) should not pollute suffix data."""
        ref = [
            _make_app_ref_row("--0", first_name="Dummy", surname="Entry"),
            _make_app_ref_row("KG1-100-0", first_name="John", surname="Smith"),
        ]
        update = [_make_council_row_ct("KG1", "100.5", "New", "Person",
                                        "Flat 3", "High Road", "NW10 3JU", "N")]
        _, rows, _ = self._run_with_app_ref(update, ref,
                                             update_headers=_PAD_COUNCIL_HEADERS_CT)
        # Should get suffix 1 (skipping 0 from KG1-100), not affected by --0 dummy
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")

    def test_post_code_with_space_handled(self):
        """App-export 'Post Code' (with space) works for padding grouping."""
        ref = [_make_app_ref_row(f"KG1-{i}-0", house_name=f"Flat {i}",
                                  road="30 High Road", post_code="NW10 3JU")
               for i in range(1, 21)]
        update = [_make_council_row("KG1", "99", "New", "Person",
                                     "Flat 5", "30 High Road", "NW10 3JU")]
        _, rows, _ = self._run_with_app_ref(update, ref)
        self.assertEqual(rows[0]["Address1"], "Flat 05")

    def test_malformed_voter_numbers_in_reference(self):
        """Reference with malformed Voter Numbers should not crash."""
        ref = [
            _make_app_ref_row("KG1-100-0", first_name="John", surname="Smith"),
            _make_app_ref_row("", first_name="Bad", surname="Entry"),
            _make_app_ref_row("malformed", first_name="Also", surname="Bad"),
        ]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run_with_app_ref(update, ref,
                                             update_headers=_PAD_COUNCIL_HEADERS_CT)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_no_record_sentinel_in_reference_names(self):
        """Reference with <NO RECORD> names falls back to address matching."""
        ref = [
            _make_app_ref_row("KG1-100-0", first_name="<NO RECORD>",
                               surname="<NO RECORD>",
                               house_name="Flat 1", road="High Road"),
            _make_app_ref_row("KG1-100-1", first_name="Mary", surname="Jones",
                               house_name="Flat 2", road="High Road"),
        ]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run_with_app_ref(update, ref,
                                             update_headers=_PAD_COUNCIL_HEADERS_CT)
        # Should match suffix 0 via address since name can't help
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_empty_reference_fields(self):
        """Reference rows with all empty address fields don't crash."""
        ref = [
            _make_app_ref_row("KG1-100-0", first_name="John", surname="Smith",
                               house_name="", house_number="", road=""),
        ]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run_with_app_ref(update, ref,
                                             update_headers=_PAD_COUNCIL_HEADERS_CT)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")


# ---------------------------------------------------------------------------
# ChangeTypeID-aware suffix tests
# ---------------------------------------------------------------------------

_PAD_COUNCIL_HEADERS_CT = _PAD_COUNCIL_HEADERS + ["ChangeTypeID"]


def _make_council_row_ct(pdcode, rollno, surname, forename, addr1, addr2, postcode,
                         change_type="N"):
    base = {h: "" for h in _PAD_COUNCIL_HEADERS_CT}
    base.update({"PDCode": pdcode, "RollNo": rollno, "ElectorSurname": surname,
                 "ElectorForename": forename, "RegisteredAddress1": addr1,
                 "RegisteredAddress2": addr2, "PostCode": postcode,
                 "ChangeTypeID": change_type})
    return base


class TestChangeTypeID(unittest.TestCase):
    """Tests for ChangeTypeID-aware suffix handling with --full-register."""

    def _make_ref_row(self, prefix, number, suffix, surname="Test", forename="Test",
                      addr1="Flat 1", addr2="High Road"):
        row = _make_ttw_row(addr1, addr2, "NW10 3JU")
        row["Elector No. Prefix"] = prefix
        row["Elector No."] = number
        row["Elector No. Suffix"] = suffix
        row["Full Elector No."] = f"{prefix}-{number}-{suffix}" if suffix else f"{prefix}-{number}"
        row["Surname"] = surname
        row["Forename"] = forename
        return row

    def _run(self, update_rows, reference_rows):
        update_path = _write_temp_csv(update_rows, _PAD_COUNCIL_HEADERS_CT)
        ref_path = _write_temp_csv(reference_rows, _PAD_TTW_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        fd2, report_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd2)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      extra_args=["--full-register", ref_path],
                                      report_file=report_path)
            self.assertEqual(rc, 0, stderr)
            headers, rows = read_output_csv(out_path)
            report_text = Path(report_path).read_text(encoding="utf-8")
            return headers, rows, report_text
        finally:
            for p in [update_path, ref_path, out_path, report_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_amend_gets_reference_suffix(self):
        """A row with same prefix-number as reference gets the reference's suffix."""
        ref = [self._make_ref_row("KG1", "100", "0", surname="Smith", forename="John")]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_delete_gets_reference_suffix(self):
        """D row gets the reference's suffix."""
        ref = [self._make_ref_row("KG1", "200", "1", surname="Jones", forename="Mary")]
        update = [_make_council_row_ct("KG1", "200", "Jones", "Mary",
                                        "Flat 2", "High Road", "NW10 3JU", "D")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")

    def test_amend_multiple_ref_picks_best_by_name(self):
        """A row with 2 ref candidates picks the one with matching name."""
        ref = [
            self._make_ref_row("KG1", "100", "0", surname="Smith", forename="John",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("KG1", "100", "1", surname="Smith", forename="Jane",
                               addr1="Flat 1", addr2="High Road"),
        ]
        update = [_make_council_row_ct("KG1", "100", "Smith", "Jane",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")

    def test_amend_name_change_uses_address(self):
        """A row where name differs (being amended) but address matches."""
        ref = [
            self._make_ref_row("KG1", "100", "0", surname="Smyth", forename="John",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("KG1", "100", "1", surname="Jones", forename="Mary",
                               addr1="Flat 2", addr2="High Road"),
        ]
        # Amending "Smyth" to "Smith" — name doesn't match but address does
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_ad_no_reference_warns(self):
        """A/D row with no matching reference → ORPHAN-{row_num} sentinel + critical warning."""
        ref = [self._make_ref_row("KG1", "999", "0")]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, report = self._run(update, ref)
        self.assertTrue(rows[0]["Elector No. Suffix"].startswith("ORPHAN-"),
                        f"Expected ORPHAN-* sentinel, got {rows[0]['Elector No. Suffix']!r}")
        self.assertIn("orphan", report.lower())
        self.assertIn("manual check required", report.lower())

    def test_new_row_skips_ad_suffix(self):
        """N row doesn't get a suffix already assigned to an A/D row."""
        ref = [self._make_ref_row("KG1", "100", "0", surname="Smith", forename="John")]
        update = [
            _make_council_row_ct("KG1", "100", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "100.5", "New", "Person",
                                  "Flat 3", "High Road", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        amend_row = [r for r in rows if r["Surname"] == "Smith"][0]
        new_row = [r for r in rows if r["Surname"] == "New"][0]
        self.assertEqual(amend_row["Elector No. Suffix"], "0")
        self.assertNotEqual(new_row["Elector No. Suffix"], "0")

    def test_mixed_nad_batch(self):
        """Batch with N, A, and D rows — all handled correctly."""
        ref = [
            self._make_ref_row("KG1", "100", "0", surname="Smith", forename="John"),
            self._make_ref_row("KG1", "200", "0", surname="Jones", forename="Mary"),
        ]
        update = [
            _make_council_row_ct("KG1", "100", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "200", "Jones", "Mary",
                                  "Flat 2", "High Road", "NW10 3JU", "D"),
            _make_council_row_ct("KG1", "300", "New", "Voter",
                                  "Flat 3", "High Road", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        amend = [r for r in rows if r["Surname"] == "Smith"][0]
        delete = [r for r in rows if r["Surname"] == "Jones"][0]
        new = [r for r in rows if r["Surname"] == "New"][0]
        self.assertEqual(amend["Elector No. Suffix"], "0")
        self.assertEqual(delete["Elector No. Suffix"], "0")
        self.assertEqual(new["Elector No. Suffix"], "0")  # 300 has no ref, so suffix 0 is fine

    def test_dedup_skips_ad_rows(self):
        """A/D rows not reassigned by dedup even if they collide with N rows."""
        ref = [self._make_ref_row("KG1", "100", "0", surname="Smith", forename="John")]
        update = [
            _make_council_row_ct("KG1", "100", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "100", "New", "Person",
                                  "Flat 3", "High Road", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        amend = [r for r in rows if r["Surname"] == "Smith"][0]
        new = [r for r in rows if r["Surname"] == "New"][0]
        # A row keeps suffix 0 (from reference), N row gets a different suffix
        self.assertEqual(amend["Elector No. Suffix"], "0")
        self.assertNotEqual(new["Elector No. Suffix"], "0")

    def test_no_change_type_unchanged(self):
        """Without ChangeTypeID column, behaviour identical to current."""
        ref = [self._make_ref_row("KG1", "100", "0")]
        update = [_make_council_row("KG1", "100.5", "Test", "A",
                                     "Flat 1", "High Road", "NW10 3JU")]
        # Uses _make_council_row (no ChangeTypeID), NOT _make_council_row_ct
        update_path = _write_temp_csv(update, _PAD_COUNCIL_HEADERS)
        ref_path = _write_temp_csv(ref, _PAD_TTW_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      extra_args=["--full-register", ref_path])
            self.assertEqual(rc, 0, stderr)
            _, rows = read_output_csv(out_path)
            # Without ChangeTypeID, decimal suffix logic runs as before
            self.assertEqual(rows[0]["Elector No. Suffix"], "1")
        finally:
            for p in [update_path, ref_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_case_insensitive_change_type(self):
        """Lowercase 'a' works same as 'A'."""
        ref = [self._make_ref_row("KG1", "100", "0", surname="Smith", forename="John")]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "a")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_integer_rollno_ad_rows(self):
        """A/D rows with integer RollNos (the common case) matched correctly."""
        ref = [self._make_ref_row("KG1", "500", "0", surname="Patel", forename="Priya")]
        update = [_make_council_row_ct("KG1", "500", "Patel", "Priya",
                                        "45 Chamberlayne Road", "", "NW10 3JU", "A")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")
        self.assertEqual(rows[0]["Full Elector No."], "KG1-500-0")

    def test_change_type_without_reference_exits(self):
        """ChangeTypeID in input without --full-register should fail with clear error."""
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        update_path = _write_temp_csv(update, _PAD_COUNCIL_HEADERS_CT)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_clean(update_path, out_path)
            self.assertNotEqual(rc, 0)
            self.assertIn("--full-register", stderr)
        finally:
            for p in [update_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_multi_ward_update(self):
        """Update contains rows from 3 different wards; each resolves independently."""
        ref = [
            self._make_ref_row("KG1", "100", "0", surname="Patel", forename="Priya",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("BR1", "200", "0", surname="Okonkwo", forename="Chidi",
                               addr1="12 Mora Road", addr2="Brondesbury"),
            self._make_ref_row("QP1", "300", "0", surname="Murphy", forename="Siobhan",
                               addr1="5 Station Road", addr2=""),
        ]
        update = [
            _make_council_row_ct("KG1", "100", "Patel", "Priya",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("BR1", "200", "Okonkwo", "Chidi",
                                  "12 Mora Road", "Brondesbury", "NW2 6TD", "D"),
            _make_council_row_ct("QP1", "300.5", "Nguyen", "Linh",
                                  "Flat 2", "Station Road", "HA0 4AX", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        patel = [r for r in rows if r["Surname"] == "Patel"][0]
        okonkwo = [r for r in rows if r["Surname"] == "Okonkwo"][0]
        nguyen = [r for r in rows if r["Surname"] == "Nguyen"][0]
        self.assertEqual(patel["Elector No. Suffix"], "0")
        self.assertEqual(okonkwo["Elector No. Suffix"], "0")
        self.assertNotEqual(nguyen["Elector No. Suffix"], "0")

    def test_diverse_names_matching(self):
        """Names with apostrophes, hyphens, and multi-word surnames match correctly."""
        ref = [
            self._make_ref_row("KG1", "100", "0", surname="O'Brien", forename="Sean",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("KG1", "100", "1", surname="O'Brien", forename="Shaun",
                               addr1="Flat 1", addr2="High Road"),
        ]
        update = [_make_council_row_ct("KG1", "100", "O'Brien", "Sean",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_amend_and_delete_same_elector_number(self):
        """A and D rows at same prefix-number, each matched to correct reference entry."""
        ref = [
            self._make_ref_row("KG1", "100", "0", surname="Smith", forename="John",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("KG1", "100", "1", surname="Jones", forename="Mary",
                               addr1="Flat 1", addr2="High Road"),
        ]
        update = [
            _make_council_row_ct("KG1", "100", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "100", "Jones", "Mary",
                                  "Flat 1", "High Road", "NW10 3JU", "D"),
        ]
        _, rows, _ = self._run(update, ref)
        smith = [r for r in rows if r["Surname"] == "Smith"][0]
        jones = [r for r in rows if r["Surname"] == "Jones"][0]
        self.assertEqual(smith["Elector No. Suffix"], "0")
        self.assertEqual(jones["Elector No. Suffix"], "1")

    def test_many_decimal_suffixes_skip_taken(self):
        """5 new decimal rows with reference having suffixes 0, 1, 2 → assigned 3, 4, 5, 6, 7."""
        ref = [
            self._make_ref_row("KG1", "100", "0"),
            self._make_ref_row("KG1", "100", "1"),
            self._make_ref_row("KG1", "100", "2"),
        ]
        update = [
            _make_council_row_ct("KG1", "100.1", "A", "Person", "F1", "Rd", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "100.2", "B", "Person", "F2", "Rd", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "100.3", "C", "Person", "F3", "Rd", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "100.5", "D", "Person", "F4", "Rd", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "100.75", "E", "Person", "F5", "Rd", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        suffixes = sorted([r["Elector No. Suffix"] for r in rows])
        # Should get 3,4,5,6,7 (skipping 0,1,2 from reference)
        self.assertEqual(suffixes, ["3", "4", "5", "6", "7"])

    def test_large_rollno(self):
        """Large RollNo values (9999) work correctly."""
        ref = [self._make_ref_row("KG1", "9999", "0", surname="Test", forename="Person")]
        update = [_make_council_row_ct("KG1", "9999.5", "New", "Person",
                                        "Flat 1", "High Road", "NW10 3JU", "N")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No."], "9999")
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")

    def test_unknown_change_type_treated_as_new(self):
        """Unknown ChangeTypeID value treated as N (new)."""
        ref = [self._make_ref_row("KG1", "100", "0")]
        update = [_make_council_row_ct("KG1", "100.5", "New", "Person",
                                        "Flat 1", "High Road", "NW10 3JU", "X")]
        _, rows, _ = self._run(update, ref)
        # Should be treated as N row, suffix should skip 0
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")

    def test_hyphenated_surname_matching(self):
        """Hyphenated surname matches correctly."""
        ref = [self._make_ref_row("KG1", "100", "0", surname="Okonkwo-Smith",
                                  forename="Ngozi", addr1="Flat 3", addr2="Chamberlayne Road")]
        update = [_make_council_row_ct("KG1", "100", "Okonkwo-Smith", "Ngozi",
                                        "Flat 3", "Chamberlayne Road", "NW10 3JU", "A")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    def test_realistic_mixed_batch(self):
        """Realistic multi-ward batch with diverse names, mixed N/A/D, decimal and integer RollNos."""
        ref = [
            self._make_ref_row("KG1", "1416", "0", surname="Begum", forename="Fatima",
                               addr1="Flat 3", addr2="45 Chamberlayne Road"),
            self._make_ref_row("KG1", "1416", "1", surname="Begum", forename="Rashid",
                               addr1="Flat 3", addr2="45 Chamberlayne Road"),
            self._make_ref_row("HP1", "502", "0", surname="De Silva", forename="Kumara",
                               addr1="22 Craven Park Road", addr2=""),
            self._make_ref_row("HP1", "503", "0", surname="O'Connor", forename="Siobhan",
                               addr1="24 Craven Park Road", addr2=""),
        ]
        update = [
            # A: amending Fatima Begum's name spelling
            _make_council_row_ct("KG1", "1416", "Begum", "Fatimah",
                                  "Flat 3", "45 Chamberlayne Road", "NW10 3JU", "A"),
            # D: deleting Rashid Begum (moved out)
            _make_council_row_ct("KG1", "1416", "Begum", "Rashid",
                                  "Flat 3", "45 Chamberlayne Road", "NW10 3JU", "D"),
            # N: new person at same address
            _make_council_row_ct("KG1", "1416.5", "Ahmed", "Yasmin",
                                  "Flat 3", "45 Chamberlayne Road", "NW10 3JU", "N"),
            # D: deleting O'Connor
            _make_council_row_ct("HP1", "503", "O'Connor", "Siobhan",
                                  "24 Craven Park Road", "", "NW10 4AB", "D"),
            # N: new person in HP1
            _make_council_row_ct("HP1", "600", "Nguyen", "Linh",
                                  "Flat 1", "10 High Road Willesden", "NW10 2PB", "N"),
        ]
        _, rows, _ = self._run(update, ref)

        fatimah = [r for r in rows if r["Forename"] == "Fatimah"][0]
        rashid = [r for r in rows if r["Forename"] == "Rashid"][0]
        yasmin = [r for r in rows if r["Surname"] == "Ahmed"][0]
        oconnor = [r for r in rows if r["Surname"] == "O'Connor"][0]
        nguyen = [r for r in rows if r["Surname"] == "Nguyen"][0]

        # Fatimah matched to ref suffix 0 (name+address, despite spelling change)
        self.assertEqual(fatimah["Elector No. Suffix"], "0")
        self.assertEqual(fatimah["Elector No. Prefix"], "KG1")
        # Rashid matched to ref suffix 1
        self.assertEqual(rashid["Elector No. Suffix"], "1")
        # Yasmin is new, skips 0 and 1 (taken by A/D rows)
        self.assertEqual(yasmin["Elector No. Suffix"], "2")
        self.assertEqual(yasmin["Elector No."], "1416")
        # O'Connor matched to ref
        self.assertEqual(oconnor["Elector No. Suffix"], "0")
        self.assertEqual(oconnor["Elector No. Prefix"], "HP1")
        # Nguyen is new, no ref collision at HP1-600
        self.assertEqual(nguyen["Elector No. Suffix"], "0")


# ---------------------------------------------------------------------------
# Plan-mandated regression tests for decimal stripping, A/D matching, and
# orphan handling (PLAN_clean_register_updates.md tests 1-17)
# ---------------------------------------------------------------------------


class TestDecimalAndOrphanHandling(unittest.TestCase):
    """Tests for the universal decimal-strip + Phase A/B suffix rework."""

    def _make_ref_row(self, prefix, number, suffix, surname="Test", forename="Test",
                      addr1="Flat 1", addr2="High Road", postcode="NW10 3JU"):
        row = _make_ttw_row(addr1, addr2, postcode)
        row["Elector No. Prefix"] = prefix
        row["Elector No."] = number
        row["Elector No. Suffix"] = suffix
        row["Full Elector No."] = f"{prefix}-{number}-{suffix}" if suffix else f"{prefix}-{number}"
        row["Surname"] = surname
        row["Forename"] = forename
        return row

    def _run(self, update_rows, reference_rows, council_headers=None):
        headers = council_headers or _PAD_COUNCIL_HEADERS_CT
        update_path = _write_temp_csv(update_rows, headers)
        ref_path = _write_temp_csv(reference_rows, _PAD_TTW_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        fd2, report_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd2)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      extra_args=["--full-register", ref_path],
                                      report_file=report_path)
            self.assertEqual(rc, 0, stderr)
            headers_out, rows = read_output_csv(out_path)
            report_text = Path(report_path).read_text(encoding="utf-8")
            return headers_out, rows, report_text
        finally:
            for p in [update_path, ref_path, out_path, report_path]:
                if os.path.exists(p):
                    os.unlink(p)

    # --- Test 1: decimal stripping universal (no ChangeTypeID) ---
    def test_1_decimal_stripped_no_change_type(self):
        """N row with RollNo 5678.0 (no ChangeTypeID) → Elector No. = 5678."""
        update = [_make_council_row("KG1", "5678.0", "Test", "Person",
                                     "Flat 1", "High Road", "NW10 3JU")]
        update_path = _write_temp_csv(update, _PAD_COUNCIL_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_clean(update_path, out_path)
            self.assertEqual(rc, 0, stderr)
            _, rows = read_output_csv(out_path)
            self.assertEqual(rows[0]["Elector No."], "5678")
            self.assertNotIn(".", rows[0]["Elector No."])
        finally:
            for p in [update_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    # --- Test 2: A row, single candidate ---
    def test_2_amend_single_candidate(self):
        """A row with one ref candidate at 1234-2 → suffix = '2', no warning."""
        ref = [self._make_ref_row("KG1", "1234", "2", surname="Smith", forename="John")]
        update = [_make_council_row_ct("KG1", "1234.1", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, report = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No."], "1234")
        self.assertEqual(rows[0]["Elector No. Suffix"], "2")
        self.assertNotIn("low confidence", report.lower())

    # --- Test 3: multi-candidate confident ---
    def test_3_amend_confident_multi(self):
        """Two ref candidates, name+address clearly favours one → no warning."""
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Smith", forename="John",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("KG1", "1234", "1", surname="Wong", forename="Mei",
                               addr1="Flat 2", addr2="Low Road"),
        ]
        update = [_make_council_row_ct("KG1", "1234", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, report = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")
        self.assertNotIn("low confidence", report.lower())

    # --- Test 4: ambiguous score-only failure ---
    def test_4_amend_ambiguous_score_only(self):
        """Score-only failure: best ~0.4 (< 0.6), second ~0.0 (margin ~0.4 >= 0.15).

        Names are entirely dissimilar across update + both refs (name_sim=0),
        so the score is driven purely by address. ref0 has 2/3 address tokens
        in common with update → addr_sim=0.8 → score=0.4. ref1 shares nothing
        → score=0.0. Locks in the score-clause path: low score, wide margin.
        """
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Cccc", forename="Dddd",
                               addr1="222 Foo Road", addr2="",
                               postcode="NW10 3JU"),
            self._make_ref_row("KG1", "1234", "1", surname="Eeee", forename="Ffff",
                               addr1="999 Bar Lane", addr2="",
                               postcode="NW10 3JU"),
        ]
        update = [_make_council_row_ct("KG1", "1234", "Aaaa", "Bbbb",
                                        "111 Foo Road", "", "NW10 3JU", "A")]
        _, _, report = self._run(update, ref)
        self.assertIn("low confidence", report.lower())
        # Lock the score-clause path: extract reported best score, assert < 0.6
        # AND margin >= 0.15 (so the warning fired on the score clause, not margin).
        m = re.search(r"score=([\d.]+),\s*margin=([\d.]+)", report)
        self.assertIsNotNone(m, f"score/margin not found in report:\n{report}")
        score, margin = float(m.group(1)), float(m.group(2))
        self.assertLess(score, 0.6, f"score-clause should fire: score={score}")
        self.assertGreaterEqual(margin, 0.15,
            f"margin must NOT also be the reason: margin={margin}")

    # --- Test 4b: ambiguous margin-only failure ---
    def test_4b_amend_ambiguous_margin_only(self):
        """Both candidates score similarly high → margin too small → warn."""
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Smith", forename="John",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("KG1", "1234", "1", surname="Smith", forename="Jon",
                               addr1="Flat 1", addr2="High Road"),
        ]
        update = [_make_council_row_ct("KG1", "1234", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, _, report = self._run(update, ref)
        self.assertIn("low confidence", report.lower())
        # Lock the margin-clause path: best score must be >= 0.6 (so score
        # clause does NOT fire) AND margin < 0.15 (so margin clause does fire).
        m = re.search(r"score=([\d.]+),\s*margin=([\d.]+)", report)
        self.assertIsNotNone(m, f"score/margin not found in report:\n{report}")
        score, margin = float(m.group(1)), float(m.group(2))
        self.assertGreaterEqual(score, 0.6,
            f"score must NOT also be the reason: score={score}")
        self.assertLess(margin, 0.15, f"margin-clause should fire: margin={margin}")

    # --- Test 4c: confident with margin (negative anchor) ---
    def test_4c_amend_confident_with_margin(self):
        """High score AND wide margin → no warning."""
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Smith", forename="John",
                               addr1="Flat 1", addr2="High Road"),
            self._make_ref_row("KG1", "1234", "1", surname="Wong", forename="Mei",
                               addr1="Flat 2", addr2="Low Road"),
        ]
        update = [_make_council_row_ct("KG1", "1234", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, report = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")
        self.assertNotIn("low confidence", report.lower())

    # --- Test 5: D row, orphan ---
    def test_5_delete_orphan(self):
        """D row with no ref candidate → ORPHAN-{row_num} sentinel + critical warning."""
        ref = [self._make_ref_row("KG1", "999", "0")]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "D")]
        _, rows, report = self._run(update, ref)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["Elector No. Suffix"].startswith("ORPHAN-"),
                        f"got {rows[0]['Elector No. Suffix']!r}")
        self.assertIn("orphan d", report.lower())

    # --- Test 6: A row, orphan ---
    def test_6_amend_orphan(self):
        """A row with no ref candidate → ORPHAN-{row_num} sentinel + critical warning."""
        ref = [self._make_ref_row("KG1", "999", "0")]
        update = [_make_council_row_ct("KG1", "100", "Smith", "John",
                                        "Flat 1", "High Road", "NW10 3JU", "A")]
        _, rows, report = self._run(update, ref)
        self.assertTrue(rows[0]["Elector No. Suffix"].startswith("ORPHAN-"))
        self.assertIn("orphan a", report.lower())

    # --- Test 7: N rows + reference suffixes, decimals stripped ---
    def test_7_n_rows_skip_reference_suffixes(self):
        """Ref has (KG1, 1234) suffixes {0,1}; two N rows at 1234.0 and 1234.1 → 2, 3."""
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Existing", forename="One"),
            self._make_ref_row("KG1", "1234", "1", surname="Existing", forename="Two"),
        ]
        update = [
            _make_council_row_ct("KG1", "1234.0", "New", "Alpha",
                                  "Flat 3", "High Road", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "1234.1", "New", "Beta",
                                  "Flat 4", "High Road", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        for r in rows:
            self.assertEqual(r["Elector No."], "1234")
        suffixes = sorted(r["Elector No. Suffix"] for r in rows)
        self.assertEqual(suffixes, ["2", "3"])

    # --- Test 8: mixed A + N in same group ---
    def test_8_mixed_a_and_n(self):
        """Ref at 1234-{0,1}; A matches 1234-1; two N rows → A keeps 1, N gets 2, 3."""
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Smith", forename="John"),
            self._make_ref_row("KG1", "1234", "1", surname="Jones", forename="Mary"),
        ]
        update = [
            _make_council_row_ct("KG1", "1234", "Jones", "Mary",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "1234.5", "New", "One",
                                  "Flat 1", "High Road", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "1234.7", "New", "Two",
                                  "Flat 1", "High Road", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        jones = [r for r in rows if r["Surname"] == "Jones"][0]
        news = sorted([r for r in rows if r["Surname"] == "New"],
                      key=lambda r: r["Forename"])
        self.assertEqual(jones["Elector No. Suffix"], "1")
        self.assertEqual([r["Elector No. Suffix"] for r in news], ["2", "3"])

    # --- Test 9: brand-new (prefix, number) for N row ---
    def test_9_n_brand_new_key(self):
        """N row at (prefix, number) absent from ref → suffix '0'."""
        ref = [self._make_ref_row("KG1", "9999", "0")]
        update = [_make_council_row_ct("KG1", "1234", "New", "Person",
                                        "Flat 1", "High Road", "NW10 3JU", "N")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "0")

    # --- Test 10: two N rows brand-new key ---
    def test_10_two_n_brand_new(self):
        """Two N rows at brand-new (prefix, number) → suffixes '0' and '1'."""
        ref = [self._make_ref_row("KG1", "9999", "0")]
        update = [
            _make_council_row_ct("KG1", "1234", "New", "Alpha",
                                  "Flat 1", "High Road", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "1234", "New", "Beta",
                                  "Flat 2", "High Road", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        suffixes = sorted(r["Elector No. Suffix"] for r in rows)
        self.assertEqual(suffixes, ["0", "1"])

    # --- Test 11: cross-file padding regression (postcode case mismatch) ---
    def test_11_cross_file_padding_postcode_normalised(self):
        """Reference postcode 'NW10 3LB', update 'nw103lb'; ref Flat 12 → update Flat 03."""
        ref = []
        for n in range(1, 13):
            ref.append(self._make_ref_row("KG1", str(100 + n), "0",
                                           surname=f"Existing{n}", forename="Resident",
                                           addr1=f"Flat {n:02d}",
                                           addr2="High Road",
                                           postcode="NW10 3LB"))
        update = [_make_council_row_ct("KG1", "200", "New", "Person",
                                        "Flat 3", "High Road", "nw103lb", "N")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Address1"], "Flat 03")

    # --- Test 12: determinism (same input twice → byte-identical output) ---
    def test_12_determinism(self):
        """Running the cleaner on the same council-format input twice produces
        byte-identical output."""
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Smith", forename="John"),
            self._make_ref_row("KG1", "1234", "1", surname="Jones", forename="Mary"),
        ]
        update = [
            _make_council_row_ct("KG1", "1234", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "1234.5", "New", "Person",
                                  "Flat 1", "High Road", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "1234.7", "Other", "Person",
                                  "Flat 1", "High Road", "NW10 3JU", "N"),
        ]
        update_path = _write_temp_csv(update, _PAD_COUNCIL_HEADERS_CT)
        ref_path = _write_temp_csv(ref, _PAD_TTW_HEADERS)
        try:
            outputs = []
            for _ in range(2):
                fd, out_path = tempfile.mkstemp(suffix=".csv")
                os.close(fd)
                try:
                    rc, _, stderr = run_clean(update_path, out_path,
                                              extra_args=["--full-register", ref_path])
                    self.assertEqual(rc, 0, stderr)
                    outputs.append(Path(out_path).read_bytes())
                finally:
                    if os.path.exists(out_path):
                        os.unlink(out_path)
            self.assertEqual(outputs[0], outputs[1])
        finally:
            for p in [update_path, ref_path]:
                if os.path.exists(p):
                    os.unlink(p)

    # --- Test 13: two orphans at same (prefix, number) ---
    def test_13_two_orphans_same_key(self):
        """Two orphan A/D at same (prefix, number) → distinct ORPHAN-{row_num}; no FEN collision."""
        ref = [self._make_ref_row("KG1", "9999", "0")]
        update = [
            _make_council_row_ct("KG1", "100", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "100", "Jones", "Mary",
                                  "Flat 2", "High Road", "NW10 3JU", "D"),
        ]
        _, rows, report = self._run(update, ref)
        self.assertEqual(len(rows), 2)
        suffixes = sorted(r["Elector No. Suffix"] for r in rows)
        # Distinct sentinels — one per row
        self.assertEqual(len(set(suffixes)), 2)
        self.assertTrue(all(s.startswith("ORPHAN-") for s in suffixes))
        # Full Elector Numbers must also differ
        fens = sorted(r["Full Elector No."] for r in rows)
        self.assertEqual(len(set(fens)), 2)
        # Two critical warnings
        self.assertGreaterEqual(report.lower().count("orphan"), 2)
        # Defensive dedup must NOT log a spurious COLLISION (per-row sentinels
        # guarantee distinct FENs, so the all-skipped branch should never fire).
        self.assertNotIn("collision", report.lower())

    # --- Test 14: N row displaced by A/D ---
    def test_14_n_displaced_by_ad(self):
        """Ref at 1234-0; A row matches 1234-0; single N row in same group → N gets '1'."""
        ref = [self._make_ref_row("KG1", "1234", "0", surname="Smith", forename="John")]
        update = [
            _make_council_row_ct("KG1", "1234", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
            _make_council_row_ct("KG1", "1234", "New", "Person",
                                  "Flat 2", "High Road", "NW10 3JU", "N"),
        ]
        _, rows, _ = self._run(update, ref)
        amend = [r for r in rows if r["Surname"] == "Smith"][0]
        new = [r for r in rows if r["Surname"] == "New"][0]
        self.assertEqual(amend["Elector No. Suffix"], "0")
        self.assertEqual(new["Elector No. Suffix"], "1")

    # --- Test 15: reference contains non-numeric suffix ---
    def test_15_non_numeric_reference_suffix(self):
        """Ref at (KG1, 1234) has suffixes {0, A}; new N row → '1' (skips '0', ignores 'A')."""
        ref = [
            self._make_ref_row("KG1", "1234", "0", surname="Existing", forename="Zero"),
            self._make_ref_row("KG1", "1234", "A", surname="Existing", forename="Alpha"),
        ]
        update = [_make_council_row_ct("KG1", "1234", "New", "Person",
                                        "Flat 1", "High Road", "NW10 3JU", "N")]
        _, rows, _ = self._run(update, ref)
        self.assertEqual(rows[0]["Elector No. Suffix"], "1")

    # --- Test 16: postcode normalisation move doesn't double-warn ---
    def test_16_postcode_warning_not_doubled(self):
        """An invalid postcode warning is reported exactly once.

        Postcode normalisation moved from Step 9 to Step 6.65 so cross-file
        padding keys can match. The earlier site must replace the later one,
        not run alongside it — otherwise a malformed postcode would emit two
        identical warnings.
        """
        ref = [self._make_ref_row("KG1", "999", "0")]
        # "ZZZ9ZZ" passes normalize_postcode's reformatter but fails its
        # UK_POSTCODE_RE check, so it returns a single warning.
        update = [_make_council_row_ct("KG1", "100", "Test", "Person",
                                        "Flat 1", "High Road", "ZZZ9ZZ", "N")]
        _, _, report = self._run(update, ref)
        # Count only the machine-readable WARNING|Field=PostCode lines so the
        # human-readable + machine-readable dual rendering doesn't inflate the
        # count. A second postcode-normalisation pass would emit a second
        # WARNING line here.
        pc_warning_lines = [ln for ln in report.splitlines()
                            if ln.startswith("WARNING|") and "Field=PostCode" in ln]
        self.assertEqual(len(pc_warning_lines), 1,
            f"postcode warning emitted {len(pc_warning_lines)} times "
            f"(expected exactly 1). Report:\n{report}")

    # --- Test 17: no `_`-prefixed key leaks into output CSV ---
    def test_17_no_internal_keys_in_output(self):
        """`_RollNoFrac` and any other internal/synthetic keys must never
        appear as CSV column headers in the output."""
        ref = [self._make_ref_row("KG1", "1234", "0", surname="Smith", forename="John")]
        update = [
            _make_council_row_ct("KG1", "1234.5", "New", "Person",
                                  "Flat 1", "High Road", "NW10 3JU", "N"),
            _make_council_row_ct("KG1", "1234", "Smith", "John",
                                  "Flat 1", "High Road", "NW10 3JU", "A"),
        ]
        headers, _, _ = self._run(update, ref)
        leaked = [h for h in headers if h.startswith("_")]
        self.assertEqual(leaked, [],
            f"internal/synthetic keys leaked into output headers: {leaked}")


# ---------------------------------------------------------------------------
# Plan-mandated tests for SubHouse/House mapping + cross-file flat padding
# (PLAN_padding_subhouse_fix.md tests 1-11 + B2 regression)
# ---------------------------------------------------------------------------


def _make_app_export_ref_row(voter_number, first_name="Test", surname="Test",
                              house_name="", house_number="", road="",
                              post_code="NW10 3JU"):
    """Build a TTW app-export-format row used for --full-register tests
    that need House Name / House Number / Road structure rather than
    pre-cleaned Address1/Address2."""
    return {
        "Voter Number": voter_number,
        "First Name": first_name,
        "Surname": surname,
        "House Name": house_name,
        "House Number": house_number,
        "Road": road,
        "Post Code": post_code,
    }


_APP_EXPORT_HEADERS = [
    "Voter Number", "First Name", "Surname",
    "House Name", "House Number", "Road", "Post Code",
]


class TestSubHouseHousePadding(unittest.TestCase):
    """Tests for SubHouse/House mapping + cross-file flat padding."""

    def _run_council_only(self, council_rows, council_headers=None):
        """Run cleaner without --full-register; council format input."""
        headers = council_headers or _PAD_COUNCIL_HEADERS
        update_path = _write_temp_csv(council_rows, headers)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        fd2, report_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd2)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      report_file=report_path)
            self.assertEqual(rc, 0, stderr)
            headers_out, rows = read_output_csv(out_path)
            report_text = Path(report_path).read_text(encoding="utf-8")
            return headers_out, rows, report_text
        finally:
            for p in [update_path, out_path, report_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def _run_with_app_ref(self, council_rows, ref_rows,
                          council_headers=None):
        """Run cleaner with --full-register pointing to a TTW app-export."""
        headers = council_headers or _PAD_COUNCIL_HEADERS
        update_path = _write_temp_csv(council_rows, headers)
        ref_path = _write_temp_csv(ref_rows, _APP_EXPORT_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        fd2, report_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd2)
        try:
            rc, _, stderr = run_clean(update_path, out_path,
                                      extra_args=["--full-register", ref_path],
                                      report_file=report_path)
            self.assertEqual(rc, 0, stderr)
            headers_out, rows = read_output_csv(out_path)
            report_text = Path(report_path).read_text(encoding="utf-8")
            return headers_out, rows, report_text
        finally:
            for p in [update_path, ref_path, out_path, report_path]:
                if os.path.exists(p):
                    os.unlink(p)

    # --- Test 1: reference width extraction with trailing building name ---
    def test_1_ref_width_with_trailing_building(self):
        """TTW reference with House Name='Flat 0302 Queensbrook Building'
        contributes width 4 to its (Address2=building, postcode) group."""
        from clean_register import build_padding_reference

        # Write a tiny app-export to a temp file and call build_padding_reference.
        ref_rows = [
            _make_app_export_ref_row("KG1-100-0",
                house_name="Flat 0302 Queensbrook Building",
                road="Wenlock Road", post_code="NW10 3JU"),
        ]
        ref_path = _write_temp_csv(ref_rows, _APP_EXPORT_HEADERS)
        try:
            flat_w, _, _, _ = build_padding_reference(ref_path)
            # _padding_key uppercases addr; postcode normaliser canonicalises.
            from clean_register import _padding_key
            key = _padding_key("Queensbrook Building", "NW10 3JU")
            self.assertIn(key, flat_w,
                f"expected padding key {key} in {sorted(flat_w.keys())}")
            self.assertEqual(flat_w[key], 4)
        finally:
            if os.path.exists(ref_path):
                os.unlink(ref_path)

    # --- Test 2: SubHouse/House → Address1/Address2, no shift needed ---
    def test_2_subhouse_house_no_shift(self):
        """SubHouse + House populated, RegisteredAddress* empty.
        Cleaned: Address1=SubHouse, Address2=House."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="", addr2="", postcode="NW10 3JU",
                                     sub_house="Flat 105",
                                     house="Queensbrook Building")]
        _, rows, _ = self._run_council_only(update)
        self.assertEqual(rows[0]["Address1"], "Flat 105")
        self.assertEqual(rows[0]["Address2"], "Queensbrook Building")

    # --- Test 3: SubHouse/House mapping with shift ---
    def test_3_subhouse_house_with_shift(self):
        """SubHouse + House + RA1 populated (RA1 NOT a dup of House).
        Cleaned: Address1=SubHouse, Address2=House, Address3=RA1."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="Foo Road", addr2="",
                                     postcode="NW10 3JU",
                                     sub_house="Flat 105",
                                     house="Queensbrook Building")]
        _, rows, _ = self._run_council_only(update)
        self.assertEqual(rows[0]["Address1"], "Flat 105")
        self.assertEqual(rows[0]["Address2"], "Queensbrook Building")
        self.assertEqual(rows[0]["Address3"], "Foo Road")

    # --- Test 4: end-to-end Queensbrook regression (sub-width inputs only) ---
    def test_4_queensbrook_cross_file_padding(self):
        """Reference has 10 Queensbrook flats at width 4; update has 3 flats
        ALL at sub-width (105, 905, 92). All three must pad to width 4."""
        ref = []
        for nnnn in ["0302", "0306", "0506", "0709", "0911",
                     "1006", "1303", "1307", "1309", "1606"]:
            ref.append(_make_app_export_ref_row(
                f"KG1-{int(nnnn) + 9000}-0",
                house_name=f"Flat {nnnn} Queensbrook Building",
                road="Wenlock Road",
                post_code="NW10 3JU"))
        update = [
            _make_council_row("KG1", "100", "New", "Alpha",
                              addr1="", addr2="", postcode="NW10 3JU",
                              sub_house="Flat 105", house="Queensbrook Building"),
            _make_council_row("KG1", "101", "New", "Beta",
                              addr1="", addr2="", postcode="NW10 3JU",
                              sub_house="Flat 905", house="Queensbrook Building"),
            _make_council_row("KG1", "102", "New", "Gamma",
                              addr1="", addr2="", postcode="NW10 3JU",
                              sub_house="Flat 92", house="Queensbrook Building"),
        ]
        _, rows, _ = self._run_with_app_ref(update, ref)
        addr1s = sorted(r["Address1"] for r in rows)
        self.assertEqual(addr1s, ["Flat 0092", "Flat 0105", "Flat 0905"])

    # --- Test 5: no SubHouse/House → unchanged behaviour ---
    def test_5_no_subhouse_unchanged(self):
        """Council row without SubHouse/House keeps RA1 → Address1."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="57 Foo Road", addr2="",
                                     postcode="NW10 3JU")]
        _, rows, _ = self._run_council_only(update)
        self.assertEqual(rows[0]["Address1"], "57 Foo Road")

    # --- Test 6: only SubHouse populated ---
    def test_6_only_subhouse(self):
        """SubHouse populated, House empty: SubHouse → Address1, RA1 → Address2."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="Foo Building", addr2="",
                                     postcode="NW10 3JU",
                                     sub_house="Flat 5")]
        _, rows, _ = self._run_council_only(update)
        self.assertEqual(rows[0]["Address1"], "Flat 5")
        self.assertEqual(rows[0]["Address2"], "Foo Building")

    # --- Test 7: only House populated ---
    def test_7_only_house(self):
        """House populated, SubHouse empty: House → Address1, RA1 → Address2."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="Foo Road", addr2="",
                                     postcode="NW10 3JU", house="57")]
        _, rows, _ = self._run_council_only(update)
        self.assertEqual(rows[0]["Address1"], "57")
        self.assertEqual(rows[0]["Address2"], "Foo Road")

    # --- Test 8: maximally-populated shift (with overflow warning) ---
    def test_8_maximally_populated_shift(self):
        """SubHouse + House + RA1-RA6 all populated AND RA1 is NOT a House dup.
        Address1=SubHouse, Address2=House, Address3-6=RA1-RA4, RA5/RA6 dropped
        with a warning."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="Foo Lane", addr2="District 5",
                                     addr3="Sub-Borough", addr4="Borough",
                                     addr5="Brent", addr6="Greater London",
                                     postcode="NW10 3JU",
                                     sub_house="Flat 5", house="Foo Court")]
        _, rows, report = self._run_council_only(update)
        self.assertEqual(rows[0]["Address1"], "Flat 5")
        self.assertEqual(rows[0]["Address2"], "Foo Court")
        self.assertEqual(rows[0]["Address3"], "Foo Lane")
        self.assertEqual(rows[0]["Address4"], "District 5")
        self.assertEqual(rows[0]["Address5"], "Sub-Borough")
        self.assertEqual(rows[0]["Address6"], "Borough")
        self.assertIn("Brent", report)
        self.assertIn("Greater London", report)
        self.assertIn("dropped", report.lower())

    # --- Test 9: idempotency on same input ---
    def test_9_idempotency_same_input(self):
        """Cleaner produces byte-identical output on the same council input
        run twice."""
        update = [
            _make_council_row("KG1", "100", "A", "One",
                              addr1="", addr2="", postcode="NW10 3JU",
                              sub_house="Flat 5", house="Foo Court"),
            _make_council_row("KG1", "101", "B", "Two",
                              addr1="Foo Lane", addr2="", postcode="NW10 3JU",
                              sub_house="Flat 6", house="Foo Court"),
        ]
        update_path = _write_temp_csv(update, _PAD_COUNCIL_HEADERS)
        try:
            outputs = []
            for _ in range(2):
                fd, out_path = tempfile.mkstemp(suffix=".csv")
                os.close(fd)
                try:
                    rc, _, stderr = run_clean(update_path, out_path)
                    self.assertEqual(rc, 0, stderr)
                    outputs.append(Path(out_path).read_bytes())
                finally:
                    if os.path.exists(out_path):
                        os.unlink(out_path)
            self.assertEqual(outputs[0], outputs[1])
        finally:
            if os.path.exists(update_path):
                os.unlink(update_path)

    # --- Test 10: alias-collision regression (TTW input → file-swap exit) ---
    def test_10_ttw_input_rejected_not_aliased(self):
        """Passing a TTW app-export as the input file triggers the file-swap
        detector and exits non-zero, rather than silently aliasing 'House Name'
        to 'House' and running the SubHouse/House shift."""
        ttw_input_rows = [{
            "Elector No. Prefix": "KG1", "Elector No.": "100",
            "Elector No. Suffix": "0", "Full Elector No.": "KG1-100-0",
            "Surname": "Test", "Forename": "Person",
            "Address1": "Flat 105", "Address2": "Queensbrook Building",
            "PostCode": "NW10 3JU", "House Name": "Flat 105 Queensbrook Building",
        }]
        ttw_headers = ["Elector No. Prefix", "Elector No.",
                       "Elector No. Suffix", "Full Elector No.",
                       "Surname", "Forename", "Address1", "Address2",
                       "PostCode", "House Name"]
        bad_input_path = _write_temp_csv(ttw_input_rows, ttw_headers)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_clean(bad_input_path, out_path)
            self.assertNotEqual(rc, 0,
                "Cleaner should refuse a TTW-format file as input")
            self.assertIn("TTW format", stderr)
        finally:
            for p in [bad_input_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    # --- Test 11a: ReferenceShape warning fires on non-standard reference ---
    def test_11a_reference_shape_warning_fires(self):
        """Reference row with House Name='Queensbrook Building' (building only)
        + House Number='105' triggers the ReferenceShape warning."""
        ref = [_make_app_export_ref_row("KG1-100-0",
            house_name="Queensbrook Building",
            house_number="105",
            road="Wenlock Road",
            post_code="NW10 3JU")]
        update = [_make_council_row("KG1", "200", "Test", "Person",
                                     addr1="Some Road", addr2="",
                                     postcode="NW10 3JU")]
        _, _, report = self._run_with_app_ref(update, ref)
        self.assertIn("ReferenceShape", report)
        self.assertIn("non-standard flat shape", report)

    # --- Test 11b: standard reference shape does NOT warn ---
    def test_11b_standard_reference_shape_no_warning(self):
        """Reference row with House Name='Flat 0302 Queensbrook Building',
        House Number='' (the canonical TTW shape) does NOT emit the warning."""
        ref = [_make_app_export_ref_row("KG1-100-0",
            house_name="Flat 0302 Queensbrook Building",
            house_number="",
            road="Wenlock Road",
            post_code="NW10 3JU")]
        update = [_make_council_row("KG1", "200", "Test", "Person",
                                     addr1="Some Road", addr2="",
                                     postcode="NW10 3JU")]
        _, _, report = self._run_with_app_ref(update, ref)
        self.assertNotIn("ReferenceShape", report)

    # --- SubHouse/House consumed (not duplicated as raw output columns) ---
    def test_subhouse_house_not_duplicated_in_output(self):
        """When SubHouse/House are folded into Address1/Address2, they must
        NOT also appear as raw passthrough columns in the output CSV — that
        would duplicate the same data in two places."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="", addr2="", postcode="NW10 3JU",
                                     sub_house="Flat 105",
                                     house="Queensbrook Building")]
        headers, rows, _ = self._run_council_only(update)
        self.assertNotIn("SubHouse", headers,
            "SubHouse must not leak into output as a raw passthrough column")
        self.assertNotIn("House", headers,
            "House must not leak into output as a raw passthrough column")
        # And the data did make it into Address1/Address2 (sanity).
        self.assertEqual(rows[0]["Address1"], "Flat 105")
        self.assertEqual(rows[0]["Address2"], "Queensbrook Building")

    # --- B2 regression: dup-branch RA6 drop must warn ---
    def test_b2_dup_branch_ra6_warning(self):
        """When RA1 is a dup of House and RA6 is populated, the cleaner must
        warn that RA6 has been dropped (it has no Address7 to land in)."""
        update = [_make_council_row("KG1", "100", "Test", "Person",
                                     addr1="Foo Court",  # dup of House
                                     addr2="Foo Lane",
                                     addr3="District 5",
                                     addr4="Borough",
                                     addr5="Brent",
                                     addr6="Greater London",  # this should drop+warn
                                     postcode="NW10 3JU",
                                     sub_house="Flat 5", house="Foo Court")]
        _, rows, report = self._run_council_only(update)
        # Address fan-out: Address1=SubHouse, Address2=House, Address3-6 = RA2..RA5
        self.assertEqual(rows[0]["Address1"], "Flat 5")
        self.assertEqual(rows[0]["Address2"], "Foo Court")
        self.assertEqual(rows[0]["Address3"], "Foo Lane")
        self.assertEqual(rows[0]["Address4"], "District 5")
        self.assertEqual(rows[0]["Address5"], "Borough")
        self.assertEqual(rows[0]["Address6"], "Brent")
        # RA6 = "Greater London" is the only thing dropped — must be flagged.
        self.assertIn("Greater London", report)
        self.assertIn("dropped", report.lower())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Check if --verify mode
    if "--verify" in sys.argv:
        parser = argparse.ArgumentParser(description="Verify conversion output")
        parser.add_argument("--verify", action="store_true")
        parser.add_argument("output", help="Output CSV to verify")
        parser.add_argument("--input", "-i", help="Input CSV for cross-validation")
        parser.add_argument("--report", "-r", help="QA report for deletion data")
        args = parser.parse_args()
        results = run_verification(args)
        sys.exit(1 if results["fail"] > 0 else 0)
    else:
        # Run unittest
        unittest.main()


if __name__ == "__main__":
    main()
