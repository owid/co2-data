import unittest
import os
import pandas as pd
from scripts import OUTPUT_DIR


class TestMakeDataset(unittest.TestCase):
    """Unit tests for the `make_dataset` module."""

    @classmethod
    def setUpClass(cls):
        cls.data = pd.read_csv(os.path.join(OUTPUT_DIR, "owid-co2-data.csv"))
        cls.codebook = pd.read_csv(os.path.join(OUTPUT_DIR, "owid-co2-codebook.csv"))
        cls.index_cols = ["country", "year", "iso_code"]

    def test_columns_in_codebook(self):
        """All columns in cleaned dataset should be in the codebook."""
        col_in_codebook = self.data.columns.isin(self.codebook["column"])
        msg = (
            "All columns should be in the codebook, but the following "
            f"columns are not: {self.data.columns[~col_in_codebook].tolist()}"
        )
        self.assertTrue(col_in_codebook.all(), msg)

    def test_column_names_no_whitespace(self):
        """All columns in cleaned dataset should not contain whitespace."""
        col_contains_space = self.data.columns.str.contains(r"\s", regex=True)
        msg = (
            "Columns should not contain whitespace, but the following "
            f"columns do: {self.data.columns[col_contains_space].tolist()}"
        )
        self.assertTrue(col_contains_space.sum() == 0, msg)

    def test_column_names_all_lowercase(self):
        """All columns in cleaned dataset should be lowercase."""
        col_is_lower = self.data.columns == self.data.columns.str.lower()
        msg = (
            "Columns should not uppercase characters, but the following "
            f"columns do: {self.data.columns[~col_is_lower].tolist()}"
        )
        self.assertTrue(col_is_lower.all(), msg)

    def test_no_nan_rows(self):
        """All rows in cleaned dataset should contain at least one non-NaN value."""
        row_all_nan = self.data.drop(columns=self.index_cols).isnull().all(axis=1)
        msg = (
            "All rows should contain at least one non-NaN value, but "
            f"{row_all_nan.sum()} row(s) contain all NaN values."
        )
        self.assertTrue(row_all_nan.sum() == 0, msg)

    def test_deprecated_country_names_removed(self):
        """Deprecated country names (e.g. "Swaziland") should not be
        present in cleaned dataset.

        Note: this method only tests for the presence of the following
            deprecated country names: ['burma', 'macedonia',
            'swaziland'].
        """
        old_names = ["burma", "macedonia", "swaziland"]
        countries = set(self.data["country"].str.lower())
        for nm in old_names:
            self.assertNotIn(
                nm,
                countries,
                f"{nm} is a deprecated country name that should not exist in "
                "the cleaned dataset, but nevertheless exists in the cleaned "
                "dataset.",
            )
