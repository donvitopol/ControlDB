import os
import tempfile
import pandas as pd
from sqlalchemy import MetaData
from typing import Optional, Union
from unittest.mock import MagicMock


from src.excel_manager import ExcelManager
from src.controldb import ControlDB


def make_temp_excel(df: pd.DataFrame, sheet: str = "Sheet1") -> str:
    """
    Create a temporary Excel file with the given DataFrame.
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    df.to_excel(temp_file.name, sheet_name=sheet, index=False)
    return temp_file.name


def read_excel(path: str, sheet: str = "Sheet1") -> pd.DataFrame:
    """
    Safely read an Excel file. Returns an empty DataFrame if the file does not exist.
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_excel(path, sheet_name=sheet)


def temp_excel_manager() -> tuple[ExcelManager, str, tempfile.TemporaryDirectory]:
    """
    Initialize an ExcelManager instance for testing with a temporary Excel file.
    """
    temp_dir = tempfile.TemporaryDirectory()
    test_path = os.path.join(temp_dir.name, "test_excel.xlsx")

    manager = ExcelManager(logLevel=10)
    manager.logger = MagicMock()  # Mock logging to avoid clutter in test output

    return manager, test_path, temp_dir


def temp_controldb(fileName: str, rootPath: str, 
                   folderSystem: str | list[str] = None, 
                   db_type: str = "mdb",
                   password: str = None, 
                   base: MetaData | list[MetaData] = None, 
                   logLevel: int = 30
    ) -> ControlDB:
    """
    Create a fully functional temporary ControlDB instance for testing.

    Parameters
    ----------
    fileName : str
        Name of the temporary database file (e.g., "test_db.mdb").
    folderSystem : str | list[str], optional
        Optional subfolder(s) to include within the temporary directory.
    db_type : str, optional
        Type of database file to create, typically "mdb". (default: "mdb")
    password : str, optional
        If provided, the database will be created and connected automatically.
    base : MetaData | list[MetaData], optional
        SQLAlchemy metadata object or list of metadata for ORM setup.
    logLevel : int, optional
        Logging verbosity level. Lower values show more debug information.
        (default: 30 — WARNING)

    Returns
    -------
    tuple
        (ControlDB instance, full database folder path, TemporaryDirectory object)

    Notes
    -----
    - The returned TemporaryDirectory must be cleaned up after the test.
    - If `password` is provided, `setup()` is called to create and connect automatically.
    - A mock logger is attached to silence log output during testing.
    - The instance is immediately ready for use once returned.
    """

    # Initialize the ControlDB instance
    db = ControlDB(
        fileName,
        rootPath=rootPath,
        folderSystem=folderSystem,
        db_type=db_type,
        logLevel=logLevel,
    )

    # Attach a mock logger to suppress output during tests
    db.logger = MagicMock()

    # If a password is supplied, create and connect automatically
    if password:
        db.setup(password=password, base=base)

    # Return instance, root path, and temp directory for cleanup
    return db


def close_db(temp_dir, db:ControlDB):        
    try:
        db.remove(exec=True)
    except Exception:
        pass
    temp_dir.cleanup()


def create_dummy_dataframe(rows: int = 5, columns: int = 3) -> pd.DataFrame:
    """
    Create a dummy DataFrame for testing purposes.
    """
    data = {f"col{i+1}": range(1, rows+1) for i in range(columns)}
    return pd.DataFrame(data)


def safe_remove_folder(db: ControlDB) -> bool:
    """
    Safely remove the database folder only if the database file has been removed.

    Parameters
    ----------
    db : ControlDB
        Instance of ControlDB to remove the folder from.

    Returns
    -------
    bool
        True if folder removed, False otherwise.
    """
    if os.path.exists(db.filePath):
        db.logger.error(
            f"Cannot remove folder because database file still exists: {db.filePath}"
        )
        return False
    return db.remove_folder(exec=True)
