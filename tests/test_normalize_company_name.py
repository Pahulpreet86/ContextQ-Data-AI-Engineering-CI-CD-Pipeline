import pytest

from aws_glue_jobs.silver_layer_transformation_glue import (
    normalize_company_name_str
)

@pytest.mark.parametrize(
    "input_name, expected",
    [
        ("Apple", "apple"),

        ("Apple, Inc.", "apple"),

        ("Microsoft Corporation", "microsoft"),
        ("Google LLC", "google"),
        ("Amazon Ltd", "amazon"),

        ("netflix", "netflix"),

        # Empty string
        ("", ""),

        # Only suffix
        ("Inc", ""),

        # None
        (None, ""),
    ]
)
def test_normalize_company_name_str(input_name, expected):
    result = normalize_company_name_str(input_name)

    assert result == expected
