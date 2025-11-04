import os
import pandas as pd
from pretty_logger import PrettyLogger, prettylog

@prettylog
class ExcelManager:
    def __init__(self, logLevel: int = 30) -> None:
        """
        Initialize ExcelManager with logging.

        Input:
        - logLevel: int, logging level (default=30, INFO)
        """
        self.logLevel = logLevel
        self.logger: PrettyLogger

    # -----------------------
    # File creation
    # -----------------------
    def create_file(self, full_path: str, sheet_name: str = "Sheet1") -> bool:
        """
        Create an empty Excel file with a single sheet.

        Input:
        - full_path: str, full path for Excel file
        - sheet_name: str, name of initial sheet

        Return:
        - True if created successfully
        - False if failed
        """
        try:
            folder = os.path.dirname(full_path)
            if folder and not os.path.exists(folder):
                os.makedirs(folder)
                self.logger.info(f"Created directory: {folder}")

            with pd.ExcelWriter(full_path, mode="w", engine="openpyxl") as writer:
                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)

            self.logger.info(f"Created Excel file at {full_path} (sheet: {sheet_name})")
            return True
        except Exception:
            self.logger.error(f"Failed to create Excel file: {full_path}", exc_info=True)
            return False

    # -----------------------
    # Upload DataFrame
    # -----------------------
    def upload_dataframe(
        self,
        full_path: str,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1",
        include_index: bool = False
    ) -> bool:
        """
        Upload a DataFrame to Excel. Creates file if missing.

        Input:
        - full_path: str, Excel file path
        - df: pd.DataFrame to write
        - sheet_name: str, target sheet
        - include_index: bool, whether to write index column

        Return:
        - True if successful
        - False if failed
        """
        try:
            if df.empty:
                self.logger.warning("Attempted to upload empty DataFrame")
                return False

            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            with pd.ExcelWriter(full_path, mode="w", engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=include_index)

            self.logger.info(f"Uploaded DataFrame to {full_path} [{sheet_name}]")
            return True
        except Exception:
            self.logger.error(f"Failed to upload DataFrame to {full_path}", exc_info=True)
            return False

    # -----------------------
    # Read Excel file
    # -----------------------
    def read_file(
        self,
        full_path: str,
        sheet_name: str = None,
        index_column: str | None = None
    ) -> pd.DataFrame | dict | None:
        """
        Read an Excel file.

        Input:
        - full_path: str, path to Excel file
        - sheet_name: str, optional sheet name
        - index_column: str, optional column to set as index

        Return:
        - DataFrame if sheet_name specified
        - dict of DataFrames if sheet_name is None
        - None if file missing or read fails
        """
        try:
            if not os.path.exists(full_path):
                self.logger.error(f"File not found: {full_path}")
                return None

            df = pd.read_excel(full_path, sheet_name=sheet_name, index_col=index_column)
            self.logger.info(f"Read Excel file {full_path} (sheet: {sheet_name or 'all'})")
            return df
        except Exception:
            self.logger.error(f"Failed to read Excel file: {full_path}", exc_info=True)
            return None

    # -----------------------
    # Update DataFrame
    # -----------------------
    def update_dataframe(
        self,
        full_path: str,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1",
        index_column: str | None = None,
        keep_index: bool = False
    ) -> bool:
        """
        Replace or create a sheet with a new DataFrame.

        Input:
        - full_path: str, Excel file path
        - df: pd.DataFrame to write
        - sheet_name: str, sheet name
        - index_column: str, optional column to use internally as index (not written)
        - keep_index: bool, whether to write DataFrame index to Excel

        Return:
        - True if successful
        - False if failed
        """
        try:
            if df.empty:
                self.logger.warning("Empty DataFrame passed to update_dataframe")
                return False

            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            df_to_save = df.copy()

            if index_column and index_column in df.columns:
                df_to_save.set_index(index_column, inplace=True)

            with pd.ExcelWriter(full_path, mode="w", engine="openpyxl") as writer:
                df_to_save.to_excel(writer, sheet_name=sheet_name, index=keep_index)

            self.logger.info(f"Updated sheet '{sheet_name}' in {full_path}")
            return True
        except Exception:
            self.logger.error(f"Failed to update sheet '{sheet_name}' in {full_path}", exc_info=True)
            return False

    # -----------------------
    # Merge DataFrame
    # -----------------------
    def merge_dataframe(
        self,
        full_path: str,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1"
    ) -> bool:
        """
        Append a DataFrame to an Excel sheet. Creates file/sheet if missing.

        Input:
        - full_path: str, Excel file path
        - df: pd.DataFrame to append
        - sheet_name: str, sheet name

        Return:
        - True if successful
        - False if failed
        """
        try:
            if df.empty:
                self.logger.warning("Empty DataFrame passed to merge_dataframe")
                return False

            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            if not os.path.exists(full_path):
                df.to_excel(full_path, sheet_name=sheet_name, index=False)
                self.logger.info(f"Created new file and sheet {full_path} [{sheet_name}]")
                return True

            # Read existing sheet
            try:
                existing_df = pd.read_excel(full_path, sheet_name=sheet_name)
            except Exception:
                existing_df = pd.DataFrame()

            combined_df = pd.concat([existing_df, df], ignore_index=True)
            with pd.ExcelWriter(full_path, mode="w", engine="openpyxl") as writer:
                combined_df.to_excel(writer, sheet_name=sheet_name, index=False)

            self.logger.info(f"Merged DataFrame into {full_path} [{sheet_name}], total rows: {len(combined_df)}")
            return True
        except Exception:
            self.logger.error(f"Failed to merge DataFrame into {full_path}", exc_info=True)
            return False

    # -----------------------
    # Get Columns
    # -----------------------
    def get_columns(self, full_path: str, sheet_name: str = None) -> list[str] | None:
        """
        Return column names from Excel sheet.

        Input:
        - full_path: str, Excel file path
        - sheet_name: str, optional sheet name

        Return:
        - list of column names if successful
        - None if file missing or read fails
        """
        try:
            if not os.path.exists(full_path):
                self.logger.warning(f"File not found: {full_path}")
                return None

            df = pd.read_excel(full_path, sheet_name=sheet_name or 0, nrows=0)
            if isinstance(df, dict):
                df = next(iter(df.values()))
            return df.columns.tolist()
        except Exception:
            self.logger.warning(f"Failed to read columns from {full_path}", exc_info=True)
            return None

    # -----------------------
    # Remove File
    # -----------------------
    def remove_file(self, full_path: str) -> bool:
        """
        Remove an Excel file from disk.

        Input:
        - full_path: str, Excel file path

        Return:
        - True if removed successfully
        - False if failed or file not found
        """
        try:
            if not os.path.exists(full_path):
                self.logger.warning(f"File not found, cannot remove: {full_path}")
                return False

            os.remove(full_path)
            self.logger.info(f"Removed Excel file: {full_path}")
            return True
        except Exception:
            self.logger.error(f"Failed to remove Excel file: {full_path}", exc_info=True)
            return False
