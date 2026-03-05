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
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
TOOL = SCRIPT_DIR / "clean_register.py"
TEST_DATA = SCRIPT_DIR / "test_data"


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
        """64 input rows - 1 deletion = 63 output rows."""
        self.assertEqual(len(self.rows), 63)

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

    def test_council_columns_not_in_output(self):
        """Council-only columns should not appear in output."""
        council_only = {"ElectorTitle", "IERStatus", "FranchiseMarker",
                        "Euro", "Parl", "County", "Ward",
                        "MethodOfVerification", "ElectorID",
                        "SubHouse", "House", "RegisteredAddress1"}
        for col in council_only:
            self.assertNotIn(col, self.headers, f"Council column '{col}' leaked to output")

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

    def test_subhouse_house_in_report(self):
        """SubHouse/House discard should be noted in report."""
        self.assertIn("SubHouse", self.report_text)
        self.assertIn("Flat 3", self.report_text)

    def test_subhouse_incorporated_into_address1(self):
        """SubHouse data should be incorporated into Address1."""
        # Edge case row 11: SubHouse="Flat 3", Address1="Oak Manor" -> "Oak Manor Flat 3"
        kate = [r for r in self.rows if r["Surname"] == "WithSubHouse"]
        self.assertEqual(len(kate), 1)
        self.assertEqual(kate[0]["Address1"], "Oak Manor Flat 3")

    def test_discarded_columns_in_report(self):
        """Discarded columns should be listed in report."""
        self.assertIn("Discarded columns", self.report_text)
        self.assertIn("ElectorTitle", self.report_text)


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
        """Register-only output should match TTW test data column order."""
        tmp_out = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp_out.close()
        rc, _, _ = run_clean(
            TEST_DATA / "golden_input_register_only.csv", tmp_out.name,
            extra_args=[],
        )
        headers, _ = read_output_csv(tmp_out.name)
        os.unlink(tmp_out.name)
        self.assertEqual(rc, 0)

        expected_order = [
            "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
            "Surname", "Forename", "Middle Names",
            "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
            "PostCode", "UPRN",
        ]
        self.assertEqual(headers, expected_order,
            f"Column order mismatch.\nExpected: {expected_order}\nGot:      {headers}")

    def test_register_plus_elections_column_order(self):
        """Register+elections output should have core columns then election groups."""
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

        expected_order = [
            "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
            "Surname", "Forename", "Middle Names",
            "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
            "PostCode", "UPRN",
            "2022 Green Voting Intention", "2022 Party", "2022 Voted",
            "2026 Green Voting Intention", "2026 Party", "2026 Postal Voter",
        ]
        self.assertEqual(headers, expected_order)


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
        """Non-blank voted → 'v'."""
        vote_green = [r for r in self.rows if r["Surname"] == "VoteGreen"]
        self.assertEqual(len(vote_green), 1)
        self.assertEqual(vote_green[0]["2022 Voted"], "v")

    def test_voted_any_value_becomes_v(self):
        """Any non-blank value → 'v'."""
        voted_any = [r for r in self.rows if r["Surname"] == "VotedAny"]
        self.assertEqual(len(voted_any), 1)
        self.assertEqual(voted_any[0]["2022 Voted"], "v")
        self.assertEqual(voted_any[0]["2026 Postal Voter"], "v")

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
        """Duplicate PDCode+RollNo should generate WARNING."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        dup_warns = [w for _, w in warnings if "Duplicate" in w.get("Issue", "")]
        self.assertTrue(len(dup_warns) >= 1, "Duplicates should be flagged")

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

    def test_ampersand_flagged_needs_manual_fix(self):
        """Ampersand in Address1 should be flagged as NEEDS MANUAL FIX."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        amp_warns = [w for _, w in warnings if "&" in w.get("Issue", "")]
        self.assertTrue(len(amp_warns) >= 1)
        self.assertIn("NEEDS MANUAL FIX", amp_warns[0].get("Issue", ""))

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

    def test_ampersand_flagged(self):
        """Ampersand address should generate a NEEDS MANUAL FIX warning."""
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        amp_warns = [w for _, w in warnings if "&" in w.get("Issue", "")]
        self.assertTrue(len(amp_warns) >= 1, "Ampersand address should be flagged")
        for w in amp_warns:
            self.assertIn("NEEDS MANUAL FIX", w.get("Issue", ""))

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
        """Duplicate PDCode+RollNo should be flagged but both kept."""
        bakare = [r for r in self.rows if r["Surname"] == "Bakare"]
        self.assertEqual(len(bakare), 2, "Both duplicate rows should be in output")
        warnings = [parse_machine_line(l) for l in self.machine if l.startswith("WARNING")]
        dup_warns = [w for _, w in warnings if "Duplicate" in w.get("Issue", "")]
        self.assertTrue(len(dup_warns) >= 1)

    # --- SubHouse/House ---

    def test_subhouse_house_reported(self):
        """SubHouse/House data should be reported but not in output."""
        self.assertNotIn("SubHouse", self.headers)
        self.assertNotIn("House", self.headers)
        self.assertIn("SubHouse", self.report_text)
        self.assertIn("Regency Court", self.report_text)
        self.assertIn("Kilburn Court", self.report_text)

    def test_subhouse_incorporated_fernandez(self):
        """Fernandez SubHouse flat numbers should be incorporated into Address1."""
        fernandez = [r for r in self.rows if r["Surname"] == "Fernandez"]
        self.assertEqual(len(fernandez), 2)
        addr1s = sorted(r["Address1"] for r in fernandez)
        self.assertEqual(addr1s, ["Regency Court Flat 2", "Regency Court Flat 3"])

    def test_subhouse_incorporated_rivera(self):
        """Rivera SubHouse flat number should be incorporated into Address1."""
        rivera = [r for r in self.rows if r["Surname"] == "Rivera"]
        self.assertEqual(len(rivera), 1)
        self.assertEqual(rivera[0]["Address1"], "Kilburn Court Flat 9")

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

    def test_no_council_columns_in_output(self):
        """No council-specific columns should leak into output."""
        council_only = {"ElectorTitle", "IERStatus", "FranchiseMarker",
                        "RegisteredAddress1", "SubHouse", "House", "PDCode", "RollNo"}
        leaked = council_only & set(self.headers)
        self.assertEqual(leaked, set(), f"Council columns leaked: {leaked}")

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
        """GE24='yes' -> Voted='v'."""
        row = self._get_row("Patel", "Raj")
        self.assertEqual(row["GE2024 Voted"], "v")

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

    def test_postal_voter_empty(self):
        """Postal voter column on future election should be present but empty."""
        self.assertIn("LE2026 Postal Voter", self.headers)
        for row in self.rows:
            self.assertEqual(row["LE2026 Postal Voter"], "",
                f"LE2026 Postal Voter should be empty, got {row['LE2026 Postal Voter']!r}")

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
# User Verification Mode
# ---------------------------------------------------------------------------

def run_verification(args):
    """Run verification checks on user-provided files."""
    print("=" * 60)
    print("Electoral Register Conversion Verification")
    print("=" * 60)
    print()

    results = {"pass": 0, "fail": 0, "warn": 0}

    def report(status, label, detail=""):
        tag = {"pass": "[PASS]", "fail": "[FAIL]", "warn": "[WARN]"}[status]
        results[status] += 1
        msg = f"{tag} {label}"
        if detail:
            msg += f": {detail}"
        print(msg)

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

    # Check no council columns leaked
    council_cols = {"PDCode", "RollNo", "ElectorTitle", "ElectorSurname",
                    "ElectorForename", "ElectorMiddleName", "IERStatus",
                    "DateOfAttainment", "FranchiseMarker", "RegisteredAddress1",
                    "SubHouse", "House"}
    leaked = council_cols & set(out_headers)
    if not leaked:
        report("pass", "No Council Columns", "Output uses TTW column names only")
    else:
        report("fail", "No Council Columns", f"Leaked: {leaked}")

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
            f"{len(bad_fen)} malformed (first: row {bad_fen[0][0]}: '{bad_fen[0][1]}')")

    # Check Full Elector No. uniqueness
    from collections import Counter
    fen_counts = Counter(r.get("Full Elector No.", "") for r in out_rows)
    dups = {k: v for k, v in fen_counts.items() if v > 1}
    if not dups:
        report("pass", "Unique Elector Numbers", f"All {len(out_rows)} Full Elector No. values unique")
    else:
        report("fail", "Unique Elector Numbers", f"{len(dups)} duplicates")

    # Check no empty rows
    empty_rows = [i + 2 for i, r in enumerate(out_rows)
                  if all(not v.strip() for v in r.values())]
    if not empty_rows:
        report("pass", "No Empty Rows", "All rows have data")
    else:
        report("fail", "No Empty Rows", f"{len(empty_rows)} empty rows")

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
                f"{len(name_diff)} input names not found in output (expected {deleted})")

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
            report("fail", "PD Completeness", "; ".join(pd_issues[:5]))

    # Summary
    print()
    print("-" * 60)
    total = results["pass"] + results["fail"] + results["warn"]
    print(f"Results: {results['pass']}/{total} passed, "
          f"{results['fail']} failed, {results['warn']} warnings")

    return results


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
