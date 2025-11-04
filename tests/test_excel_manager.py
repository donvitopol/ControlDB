import unittest
import os
import pandas as pd

from tests.utils import (
    make_temp_excel,
    read_excel,
    temp_excel_manager,
)


class TestExcelManager(unittest.TestCase):
    def setUp(self):
        """Prepare a temporary ExcelManager and test file."""
        self.manager, self.test_path, self.temp_dir = temp_excel_manager()

    def tearDown(self):
        """Clean up the temporary Excel file and directory."""
        if os.path.exists(self.test_path):
            os.remove(self.test_path)
        self.temp_dir.cleanup()

    # -----------------------
    # File creation tests
    # -----------------------
    def test_create_file_creates_excel(self):
        """Test that create_file creates a new empty Excel file with the specified sheet."""
        # Act
        result = self.manager.create_file(self.test_path, "Data")

        # Assert
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.test_path))
        df = read_excel(self.test_path, "Data")
        self.assertTrue(df.empty)

    # -----------------------
    # Upload DataFrame tests
    # -----------------------
    def test_upload_dataframe_creates_and_writes(self):
        """
        Test that upload_dataframe:
        - Creates a new file if it does not exist
        - Writes a DataFrame correctly
        - Can overwrite existing sheets without extra index columns
        """
        # Arrange
        df1 = pd.DataFrame({"ID": [1, 2], "Value": ["A", "B"]})

        # Act: first upload
        result = self.manager.upload_dataframe(self.test_path, df1, sheet_name="Sheet1")

        # Assert
        self.assertTrue(result)
        df_out = read_excel(self.test_path, sheet="Sheet1")
        pd.testing.assert_frame_equal(df_out, df1)

        # Arrange: second DataFrame to overwrite
        df2 = pd.DataFrame({"Index": [1, 2, 3], "Letter": ["A", "B", "C"]})

        # Act: overwrite existing sheet
        result = self.manager.upload_dataframe(self.test_path, df2, sheet_name="Sheet1")

        # Assert
        self.assertTrue(result)
        df_out = read_excel(self.test_path, sheet="Sheet1")
        pd.testing.assert_frame_equal(df_out, df2)

    # -----------------------
    # Read DataFrame tests
    # -----------------------
    def test_read_file_returns_dataframe(self):
        """Test read_file behavior with and without specifying a sheet name."""
        df = pd.DataFrame({"ID": [1], "X": [10]})
        df.to_excel(self.test_path, index=False)

        # Case 1: No sheet_name → expect dict
        result = self.manager.read_file(self.test_path)
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result.get("Sheet1"), pd.DataFrame)
        self.assertIn("X", result.get("Sheet1").columns)

        # Case 2: Specific sheet_name → expect DataFrame
        result = self.manager.read_file(self.test_path, sheet_name="Sheet1")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn("X", result.columns)

    def test_read_file_returns_none_for_missing_file(self):
        """Test read_file returns None for a missing file."""
        result = self.manager.read_file("non_existing.xlsx")
        self.assertIsNone(result)

    # -----------------------
    # Update DataFrame tests
    # -----------------------
    def test_update_dataframe_creates_new_file_if_missing(self):
        """
        Test that update_dataframe creates a new Excel file if missing
        and writes the DataFrame correctly.
        """
        # Arrange
        df = pd.DataFrame({"ID": [1, 2], "Value": ["A", "B"]})

        if os.path.exists(self.test_path):
            os.remove(self.test_path)

        # Act
        result = self.manager.update_dataframe(self.test_path, df, sheet_name="Sheet1")

        # Assert
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.test_path))

        df_out = read_excel(self.test_path, "Sheet1")
        pd.testing.assert_frame_equal(df.reset_index(drop=True), df_out.reset_index(drop=True), check_dtype=False)

    def test_update_dataframe_replaces_content(self):
        """
        Test that update_dataframe correctly replaces the content of an existing sheet.
        """
        # Arrange
        df1 = pd.DataFrame({"ID": [1], "Value": ["Old"]})
        df2 = pd.DataFrame({"ID": [1], "Value": ["New"]})
        df1.to_excel(self.test_path, index=False)

        # Act
        result = self.manager.update_dataframe(self.test_path, df2, "Sheet1", index_column="ID", keep_index=False)

        # Assert
        self.assertTrue(result)
        df_out = read_excel(self.test_path, "Sheet1")
        self.assertEqual(df_out.loc[0, "Value"], "New")
        self.assertListEqual(list(df_out.columns), ["Value"])

    # -----------------------
    # Merge DataFrame tests
    # -----------------------
    def test_merge_dataframe_appends_rows(self):
        """
        Test that merge_dataframe correctly appends rows to an existing Excel sheet.
        """
        # Arrange
        df1 = pd.DataFrame({"A": [1, 2]})
        df2 = pd.DataFrame({"A": [3]})
        df1.to_excel(self.test_path, index=False)

        # Act
        result = self.manager.merge_dataframe(self.test_path, df2)

        # Assert
        self.assertTrue(result)
        df_out = read_excel(self.test_path)
        self.assertEqual(len(df_out), 3)
        self.assertIn(3, df_out["A"].tolist())
        self.assertListEqual(list(df_out.columns), ["A"])

    def test_merge_then_update(self):
        """
        Test sequence of merge followed by update to ensure data consistency.
        """
        # Arrange
        df1 = pd.DataFrame({"ID": [1], "Value": ["X"]})
        df2 = pd.DataFrame({"ID": [2], "Value": ["Y"]})

        # Merge multiple times
        self.manager.merge_dataframe(self.test_path, df1)
        self.manager.merge_dataframe(self.test_path, df2)

        df_out = read_excel(self.test_path)
        self.assertEqual(len(df_out), 2)

        # Update sheet with new DataFrame
        df_new = pd.DataFrame({"ID": [3], "Value": ["Z"]})
        self.manager.update_dataframe(self.test_path, df_new, index_column="ID", keep_index=False)

        df_out = read_excel(self.test_path)
        self.assertEqual(len(df_out), 1)
        self.assertEqual(df_out.iloc[0]["Value"], "Z")

    # -----------------------
    # Column and file utility tests
    # -----------------------
    def test_get_columns_returns_correct_headers(self):
        """Test that get_columns returns correct column headers from a sheet."""
        df = pd.DataFrame({"X": [1], "Y": [2]})
        df.to_excel(self.test_path, index=False)

        cols_default = self.manager.get_columns(self.test_path)
        self.assertEqual(cols_default, ["X", "Y"])

        cols_named = self.manager.get_columns(self.test_path, sheet_name="Sheet1")
        self.assertEqual(cols_named, ["X", "Y"])

        missing = self.manager.get_columns("non_existing_file.xlsx")
        self.assertIsNone(missing)

    def test_remove_file_deletes_file(self):
        """Test that remove_file deletes an existing Excel file."""
        self.manager.create_file(self.test_path)
        self.assertTrue(os.path.exists(self.test_path))

        result = self.manager.remove_file(self.test_path)
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.test_path))
