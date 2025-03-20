import pytest

from utils.helpers import filter_out_missing_values, map_column_with_default, group_by_concat

def test_filter_out_missing_values(spark, sample_data_with_missing_values):
    """Test that rows with missing or empty values in the specified column are filtered out."""
    df = filter_out_missing_values(sample_data_with_missing_values, "PersonID")
    result = df.select("PersonID").collect()
    expected = [{"PersonID": "1"}, {"PersonID": "2"}, {"PersonID": "3"}]

    assert [row.asDict() for row in result] == expected

def test_map_column_with_default(spark, sample_data_for_mapping):
    """Test that values are mapped correctly and defaults are applied for unmapped values."""
    mapping = {"M": "Male", "F": "Female", "unknown": ""}
    df = map_column_with_default(sample_data_for_mapping, "Sex", "gender", mapping, "Other")

    result = df.select("PersonID", "gender").collect()
    expected = [
        {"PersonID": "1", "gender": "Male"},
        {"PersonID": "2", "gender": "Female"},
        {"PersonID": "3", "gender": ""},
        {"PersonID": "4", "gender": "Other"}
    ]

    assert [row.asDict() for row in result] == expected

def test_group_by_concat(spark, sample_data_for_concat):
    """Test that values in a column are concatenated correctly, grouped by another column."""
    df = group_by_concat(sample_data_for_concat, "PersonID", "problem_list", "Dx_Code")

    result = df.select("PersonID", "problem_list").collect()
    expected = [
        {"PersonID": "1", "problem_list": "Z01.818 | H35.363"},
        {"PersonID": "2", "problem_list": "E11.9 | I10"}
    ]

    assert [row.asDict() for row in result] == expected
