"""Tests for src/validator.py.

Covers all public functions and every private helper. Rules are constructed
inline as plain dicts — no YAML file I/O required except in TestLoadRules.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make src/ importable without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from validator import (
    _basename,
    _check_min_non_null,
    _check_pattern,
    _check_range,
    _check_required,
    _check_type,
    _is_empty,
    _is_valid_date,
    _is_valid_numeric,
    _is_valid_pattern,
    load_rules,
    summarize,
    validate,
)


# ---------------------------------------------------------------------------
# Minimal rules fixtures
# ---------------------------------------------------------------------------

MINIMAL_RULES: dict = {
    "columns": {
        "date": {"type": "date", "required": True, "min": "2020-01-01", "max": "2025-12-31"},
        "revenue": {"type": "numeric", "required": True, "min": 0},
        "quantity": {"type": "numeric", "required": False, "min": 1},
        "product": {"type": "text", "required": True},
        "email": {"type": "text", "required": False, "pattern": r".*@.*\..*"},
    },
    "min_non_null_fields": 3,
    "flag_non_conforming_types": True,
    "negative_revenue_allowed_files": ["returns.csv"],
}


def _make_row(**kwargs) -> pd.Series:
    """Construct a pd.Series row with source metadata defaults."""
    defaults = {
        "source_file": "test.csv",
        "source_row": 5,
        "date": "2024-01-01",
        "revenue": "100.0",
        "quantity": "2.0",
        "product": "Widget",
        "email": "alice@example.com",
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


# ---------------------------------------------------------------------------
# _is_empty
# ---------------------------------------------------------------------------


class TestIsEmpty:
    """Tests for the _is_empty predicate."""

    def test_none_is_empty(self) -> None:
        """None is treated as empty."""
        assert _is_empty(None) is True

    def test_float_nan_is_empty(self) -> None:
        """float NaN is treated as empty."""
        assert _is_empty(float("nan")) is True

    def test_empty_string_is_empty(self) -> None:
        """Empty string is treated as empty."""
        assert _is_empty("") is True

    def test_whitespace_string_is_empty(self) -> None:
        """Whitespace-only string is treated as empty."""
        assert _is_empty("   ") is True

    def test_nan_string_is_empty(self) -> None:
        """The string 'nan' (stringified pandas NaN) is treated as empty."""
        assert _is_empty("nan") is True
        assert _is_empty("NaN") is True

    def test_zero_is_not_empty(self) -> None:
        """The integer 0 is not empty."""
        assert _is_empty(0) is False

    def test_non_empty_string(self) -> None:
        """A non-blank string is not empty."""
        assert _is_empty("hello") is False

    def test_list_is_not_empty(self) -> None:
        """An unhashable type (list) is not empty."""
        assert _is_empty([1, 2]) is False


# ---------------------------------------------------------------------------
# _is_valid_numeric
# ---------------------------------------------------------------------------


class TestIsValidNumeric:
    """Tests for _is_valid_numeric."""

    def test_plain_integer_string(self) -> None:
        """'42' is valid numeric."""
        assert _is_valid_numeric("42") is True

    def test_float_string(self) -> None:
        """'3.14' is valid numeric."""
        assert _is_valid_numeric("3.14") is True

    def test_negative_float(self) -> None:
        """'-100.5' is valid numeric."""
        assert _is_valid_numeric("-100.5") is True

    def test_non_numeric_string(self) -> None:
        """'pending' is not valid numeric."""
        assert _is_valid_numeric("pending") is False

    def test_empty_string(self) -> None:
        """Empty string is not valid numeric."""
        assert _is_valid_numeric("") is False

    def test_none(self) -> None:
        """None is not valid numeric."""
        assert _is_valid_numeric(None) is False


# ---------------------------------------------------------------------------
# _is_valid_date
# ---------------------------------------------------------------------------


class TestIsValidDate:
    """Tests for _is_valid_date."""

    def test_valid_date(self) -> None:
        """'2024-03-15' is a valid date."""
        assert _is_valid_date("2024-03-15") is True

    def test_impossible_month_rejected(self) -> None:
        """Month 13 is rejected."""
        assert _is_valid_date("2024-13-01") is False

    def test_impossible_day_rejected(self) -> None:
        """Feb 30 is rejected."""
        assert _is_valid_date("2024-02-30") is False

    def test_wrong_format_rejected(self) -> None:
        """US-format date string is rejected (not normalized yet at validation time)."""
        assert _is_valid_date("03/15/2024") is False

    def test_empty_is_invalid(self) -> None:
        """Empty string is not a valid date."""
        assert _is_valid_date("") is False

    def test_none_is_invalid(self) -> None:
        """None is not a valid date."""
        assert _is_valid_date(None) is False


# ---------------------------------------------------------------------------
# _is_valid_pattern
# ---------------------------------------------------------------------------


class TestIsValidPattern:
    """Tests for _is_valid_pattern."""

    def test_matching_pattern(self) -> None:
        """A value that matches the pattern returns True."""
        assert _is_valid_pattern("alice@example.com", r".*@.*\..*") is True

    def test_non_matching_pattern(self) -> None:
        """A value that does not match returns False."""
        assert _is_valid_pattern("not-an-email", r".*@.*\..*") is False

    def test_malformed_regex_silently_passes(self) -> None:
        """A malformed regex in the config returns True to avoid crashing."""
        assert _is_valid_pattern("anything", r"[invalid") is True

    def test_fullmatch_requirement(self) -> None:
        """Pattern must cover the whole string, not just a prefix."""
        # 'abc' does not fully match '^a$'
        assert _is_valid_pattern("abc", r"^a$") is False
        assert _is_valid_pattern("a", r"^a$") is True


# ---------------------------------------------------------------------------
# _check_required
# ---------------------------------------------------------------------------


class TestCheckRequired:
    """Tests for _check_required."""

    def _rule(self, required: bool = True) -> dict:
        return {"required": required}

    def test_required_empty_fails(self) -> None:
        """Empty value for a required field returns a failure message."""
        msg = _check_required("revenue", "", self._rule(True), "5", "test.csv")
        assert msg is not None
        assert "required" in msg
        assert "revenue" in msg

    def test_required_non_empty_passes(self) -> None:
        """Non-empty value for a required field returns None."""
        msg = _check_required("revenue", "100", self._rule(True), "5", "test.csv")
        assert msg is None

    def test_optional_empty_passes(self) -> None:
        """Empty value for an optional field returns None."""
        msg = _check_required("email", "", self._rule(False), "5", "test.csv")
        assert msg is None


# ---------------------------------------------------------------------------
# _check_type
# ---------------------------------------------------------------------------


class TestCheckType:
    """Tests for _check_type."""

    def test_numeric_with_valid_value_passes(self) -> None:
        """A numeric string passes a numeric type check."""
        rule = {"type": "numeric"}
        assert _check_type("revenue", "100.0", rule, "5", "test.csv") is None

    def test_numeric_with_string_fails(self) -> None:
        """A non-numeric string fails a numeric type check."""
        rule = {"type": "numeric"}
        msg = _check_type("revenue", "TBD", rule, "5", "test.csv")
        assert msg is not None
        assert "TBD" in msg

    def test_date_with_valid_date_passes(self) -> None:
        """A valid YYYY-MM-DD date passes a date type check."""
        rule = {"type": "date"}
        assert _check_type("date", "2024-01-01", rule, "5", "test.csv") is None

    def test_date_with_invalid_date_fails(self) -> None:
        """A non-date string fails a date type check."""
        rule = {"type": "date"}
        msg = _check_type("date", "not-a-date", rule, "5", "test.csv")
        assert msg is not None
        assert "not-a-date" in msg

    def test_text_type_always_passes(self) -> None:
        """Any non-empty string passes a text type check."""
        rule = {"type": "text"}
        assert _check_type("product", "anything", rule, "5", "test.csv") is None

    def test_no_type_defaults_to_text(self) -> None:
        """A column rule with no 'type' key defaults to text and always passes."""
        rule: dict = {}
        assert _check_type("notes", "some text", rule, "5", "test.csv") is None


# ---------------------------------------------------------------------------
# _check_range
# ---------------------------------------------------------------------------


class TestCheckRange:
    """Tests for _check_range."""

    def _numeric_rule(self, min_val=None, max_val=None) -> dict:
        rule: dict = {"type": "numeric"}
        if min_val is not None:
            rule["min"] = min_val
        if max_val is not None:
            rule["max"] = max_val
        return rule

    def _date_rule(self, min_val=None, max_val=None) -> dict:
        rule: dict = {"type": "date"}
        if min_val is not None:
            rule["min"] = min_val
        if max_val is not None:
            rule["max"] = max_val
        return rule

    def test_value_below_min_fails(self) -> None:
        """A numeric value below min returns a failure message."""
        msg = _check_range("revenue", "-10", self._numeric_rule(min_val=0), "5", "test.csv", [])
        assert msg is not None
        assert "negative" in msg

    def test_value_above_max_fails(self) -> None:
        """A numeric value above max returns a failure message."""
        msg = _check_range("quantity", "999", self._numeric_rule(max_val=100), "5", "test.csv", [])
        assert msg is not None
        assert "exceeds maximum" in msg

    def test_value_within_range_passes(self) -> None:
        """A numeric value within range returns None."""
        msg = _check_range("revenue", "50", self._numeric_rule(min_val=0, max_val=100), "5", "test.csv", [])
        assert msg is None

    def test_negative_revenue_allowed_in_exempt_file(self) -> None:
        """Negative revenue is allowed when the file is in allowed_negative_files."""
        msg = _check_range(
            "revenue", "-200", self._numeric_rule(min_val=0), "5", "returns.csv",
            allowed_negative_files=["returns.csv"],
        )
        assert msg is None

    def test_negative_revenue_blocked_in_non_exempt_file(self) -> None:
        """Negative revenue is quarantined for files not in the exempt list."""
        msg = _check_range(
            "revenue", "-200", self._numeric_rule(min_val=0), "5", "regular_sales.csv",
            allowed_negative_files=["returns.csv"],
        )
        assert msg is not None

    def test_date_before_min_fails(self) -> None:
        """A date before min is flagged."""
        msg = _check_range(
            "date", "2010-01-01", self._date_rule(min_val="2020-01-01"), "5", "test.csv", []
        )
        assert msg is not None
        assert "before minimum" in msg

    def test_date_after_max_fails(self) -> None:
        """A date after max is flagged."""
        msg = _check_range(
            "date", "2030-01-01", self._date_rule(max_val="2025-12-31"), "5", "test.csv", []
        )
        assert msg is not None
        assert "after maximum" in msg

    def test_date_within_range_passes(self) -> None:
        """A date within min/max returns None."""
        msg = _check_range(
            "date", "2024-06-15",
            self._date_rule(min_val="2020-01-01", max_val="2025-12-31"),
            "5", "test.csv", [],
        )
        assert msg is None

    def test_no_min_max_always_passes(self) -> None:
        """A rule with no min or max never fails the range check."""
        msg = _check_range("revenue", "999999", {"type": "numeric"}, "5", "test.csv", [])
        assert msg is None


# ---------------------------------------------------------------------------
# _check_pattern
# ---------------------------------------------------------------------------


class TestCheckPattern:
    """Tests for _check_pattern."""

    def test_matching_value_passes(self) -> None:
        """A value matching the pattern returns None."""
        rule = {"pattern": r".*@.*\..*"}
        assert _check_pattern("email", "alice@example.com", rule, "5", "test.csv") is None

    def test_non_matching_value_fails(self) -> None:
        """A value not matching the pattern returns a failure message."""
        rule = {"pattern": r".*@.*\..*"}
        msg = _check_pattern("email", "not-an-email", rule, "5", "test.csv")
        assert msg is not None
        assert "does not match" in msg

    def test_no_pattern_always_passes(self) -> None:
        """A rule without a pattern key returns None."""
        rule: dict = {"type": "text"}
        assert _check_pattern("product", "Widget", rule, "5", "test.csv") is None


# ---------------------------------------------------------------------------
# _check_min_non_null
# ---------------------------------------------------------------------------


class TestCheckMinNonNull:
    """Tests for _check_min_non_null."""

    def test_enough_fields_passes(self) -> None:
        """Row with 3 populated fields passes min_non_null=3."""
        row = pd.Series({"date": "2024-01-01", "product": "Widget", "revenue": "100"})
        col_rules = {"date": {}, "product": {}, "revenue": {}}
        assert _check_min_non_null(row, col_rules, 3, "5", "test.csv") is None

    def test_too_few_fields_fails(self) -> None:
        """Row with only 1 populated field fails min_non_null=3."""
        row = pd.Series({"date": "2024-01-01", "product": None, "revenue": ""})
        col_rules = {"date": {}, "product": {}, "revenue": {}}
        msg = _check_min_non_null(row, col_rules, 3, "5", "test.csv")
        assert msg is not None
        assert "minimum 3 required" in msg

    def test_min_non_null_zero_always_passes(self) -> None:
        """min_non_null=0 means no minimum requirement."""
        row = pd.Series({"date": None, "product": None})
        col_rules = {"date": {}, "product": {}}
        assert _check_min_non_null(row, col_rules, 0, "5", "test.csv") is None

    def test_metadata_columns_excluded(self) -> None:
        """source_file and source_row are not counted toward min_non_null."""
        row = pd.Series(
            {
                "source_file": "test.csv",
                "source_row": 5,
                "date": "2024-01-01",
                "product": "Widget",
                "revenue": "100",
            }
        )
        # Only date, product, revenue are data columns
        col_rules = {"date": {}, "product": {}, "revenue": {}}
        assert _check_min_non_null(row, col_rules, 3, "5", "test.csv") is None


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


class TestValidate:
    """Tests for the public validate function."""

    def _df_from_rows(self, rows: list[dict]) -> pd.DataFrame:
        """Build a DataFrame from a list of row dicts."""
        return pd.DataFrame(rows)

    def test_clean_row_goes_to_clean_df(self) -> None:
        """A fully-valid row ends up in clean_df with no quarantine_reason."""
        df = self._df_from_rows([{
            "source_file": "test.csv", "source_row": 2,
            "date": "2024-01-01", "revenue": "100.0",
            "quantity": "5.0", "product": "Widget", "email": "a@b.com",
        }])
        clean, quarantine = validate(df, MINIMAL_RULES)
        assert len(clean) == 1
        assert len(quarantine) == 0

    def test_bad_row_goes_to_quarantine_df(self) -> None:
        """A row with an invalid value ends up in quarantine_df."""
        df = self._df_from_rows([{
            "source_file": "test.csv", "source_row": 2,
            "date": "2024-01-01", "revenue": "not-a-number",
            "quantity": "5.0", "product": "Widget", "email": "a@b.com",
        }])
        clean, quarantine = validate(df, MINIMAL_RULES)
        assert len(clean) == 0
        assert len(quarantine) == 1

    def test_quarantine_df_has_reason_column(self) -> None:
        """quarantine_df has a 'quarantine_reason' column as the first column."""
        df = self._df_from_rows([{
            "source_file": "test.csv", "source_row": 2,
            "date": "2024-01-01", "revenue": "bad",
            "quantity": "5.0", "product": "Widget", "email": "a@b.com",
        }])
        _, quarantine = validate(df, MINIMAL_RULES)
        assert "quarantine_reason" in quarantine.columns
        assert quarantine.columns[0] == "quarantine_reason"

    def test_mixed_rows_split_correctly(self) -> None:
        """Clean and bad rows are correctly split into separate DataFrames."""
        df = self._df_from_rows([
            {
                "source_file": "test.csv", "source_row": 2,
                "date": "2024-01-01", "revenue": "100.0",
                "quantity": "5.0", "product": "Widget", "email": "a@b.com",
            },
            {
                "source_file": "test.csv", "source_row": 3,
                "date": "2024-01-01", "revenue": "bad",
                "quantity": "5.0", "product": "Widget", "email": "a@b.com",
            },
        ])
        clean, quarantine = validate(df, MINIMAL_RULES)
        assert len(clean) == 1
        assert len(quarantine) == 1

    def test_multiple_failures_joined_by_semicolon(self) -> None:
        """Multiple failures on one row are joined with '; '."""
        df = self._df_from_rows([{
            "source_file": "test.csv", "source_row": 2,
            "date": "bad-date", "revenue": "bad-rev",
            "quantity": "5.0", "product": "Widget", "email": "a@b.com",
        }])
        _, quarantine = validate(df, MINIMAL_RULES)
        reason = quarantine["quarantine_reason"].iloc[0]
        assert ";" in reason

    def test_missing_required_field_quarantined(self) -> None:
        """A row missing a required field is quarantined."""
        df = self._df_from_rows([{
            "source_file": "test.csv", "source_row": 2,
            "date": "2024-01-01", "revenue": None,
            "quantity": "5.0", "product": "Widget", "email": "a@b.com",
        }])
        _, quarantine = validate(df, MINIMAL_RULES)
        assert len(quarantine) == 1
        assert "revenue" in quarantine["quarantine_reason"].iloc[0]

    def test_output_indices_reset(self) -> None:
        """Both output DataFrames have a clean integer RangeIndex starting at 0."""
        df = self._df_from_rows([
            {
                "source_file": "test.csv", "source_row": 2,
                "date": "2024-01-01", "revenue": "100.0",
                "quantity": "5.0", "product": "Widget", "email": "a@b.com",
            },
            {
                "source_file": "test.csv", "source_row": 3,
                "date": "2024-01-01", "revenue": "bad",
                "quantity": "5.0", "product": "Widget", "email": "a@b.com",
            },
        ])
        clean, quarantine = validate(df, MINIMAL_RULES)
        assert list(clean.index) == [0]
        assert list(quarantine.index) == [0]


# ---------------------------------------------------------------------------
# summarize
# ---------------------------------------------------------------------------


class TestSummarize:
    """Tests for the summarize function."""

    def _make_dfs(
        self, n_clean: int, n_quarantine: int, source_file: str = "test.csv"
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Build minimal clean and quarantine DataFrames for summarize()."""
        clean = pd.DataFrame(
            {"source_file": [source_file] * n_clean, "revenue": ["100"] * n_clean}
        )
        quarantine = pd.DataFrame(
            {
                "quarantine_reason": ["bad"] * n_quarantine,
                "source_file": [source_file] * n_quarantine,
                "revenue": ["x"] * n_quarantine,
            }
        )
        return clean, quarantine

    def test_summary_contains_counts(self) -> None:
        """Summary string contains correct row counts."""
        clean, quarantine = self._make_dfs(10, 2)
        msg = summarize(clean, quarantine)
        assert "10 rows passed" in msg
        assert "2 rows quarantined" in msg

    def test_summary_contains_file_count(self) -> None:
        """Summary string mentions number of files."""
        clean, quarantine = self._make_dfs(5, 1)
        msg = summarize(clean, quarantine)
        assert "1 file" in msg

    def test_per_file_breakdown_present(self) -> None:
        """Summary contains per-file line for each source file."""
        clean, quarantine = self._make_dfs(5, 1, source_file="sales.csv")
        msg = summarize(clean, quarantine)
        assert "sales.csv" in msg

    def test_zero_quarantine_phrasing(self) -> None:
        """When no rows are quarantined the summary says '0 rows quarantined'."""
        clean, quarantine = self._make_dfs(3, 0)
        msg = summarize(clean, quarantine)
        assert "0 rows quarantined" in msg

    def test_singular_phrasing_for_one_row(self) -> None:
        """Single clean row uses 'row' not 'rows'."""
        clean, quarantine = self._make_dfs(1, 0)
        msg = summarize(clean, quarantine)
        assert "1 row passed" in msg


# ---------------------------------------------------------------------------
# load_rules
# ---------------------------------------------------------------------------


class TestLoadRules:
    """Tests for load_rules against the actual config file."""

    CONFIG_PATH: Path = (
        Path(__file__).parent.parent / "config" / "validation_rules.yaml"
    )

    def test_loads_without_error(self) -> None:
        """load_rules reads the real YAML config without raising."""
        rules = load_rules(self.CONFIG_PATH)
        assert isinstance(rules, dict)

    def test_expected_top_level_keys(self) -> None:
        """The config contains the required top-level keys."""
        rules = load_rules(self.CONFIG_PATH)
        for key in ("columns", "min_non_null_fields", "flag_non_conforming_types"):
            assert key in rules, f"Missing key: {key}"

    def test_columns_section_present(self) -> None:
        """The 'columns' section contains at least date and revenue."""
        rules = load_rules(self.CONFIG_PATH)
        cols = rules["columns"]
        assert "date" in cols
        assert "revenue" in cols

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        """load_rules raises FileNotFoundError for a non-existent path."""
        with pytest.raises(FileNotFoundError):
            load_rules(tmp_path / "missing.yaml")
