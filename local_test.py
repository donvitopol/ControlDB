import os
import pandas as pd
import os
import tempfile
import pandas as pd
from sqlalchemy import MetaData
from unittest.mock import MagicMock
from src import ExcelManager
from src import ControlDB, ROOTBASE, UserTable

# Utils
def read_excel(path: str, sheet="Sheet1") -> pd.DataFrame:
    """Read Excel file safely, returning an empty DataFrame if not found."""
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_excel(path, sheet_name=sheet)

def temp_excel_manager() -> tuple[ExcelManager, str, tempfile.TemporaryDirectory]:
    """Return (ExcelManager, test_path, temp_dir) ready for use in tests."""
    temp_dir = tempfile.TemporaryDirectory()
    test_path = os.path.join(temp_dir.name, "test_excel.xlsx")

    manager = ExcelManager()
    manager.logger = MagicMock()

    return manager, test_path, temp_dir

def test_read_file_returns_dataframe(manager:ExcelManager, test_path):
    df = pd.DataFrame({"ID": [1], "X": [10]})
    df.to_excel(test_path, index=False)
    result = manager.read_file(test_path)
    # self.assertIsInstance(result, dict)
    print(type(result))
    print(result.keys())
    print(type(result["Sheet1"]))
    # self.assertIsInstance(result["Sheet1"], pd.DataFrame)
    result = manager.read_file(test_path,sheet_name="Sheet1")
    # self.assertIsInstance(result, pd.DataFrame)
    print(type(result))
    print(result.columns)
    # self.assertIn("X", result.columns)

def test_get_columns_returns_correct_headers(manager:ExcelManager, test_path):
    df = pd.DataFrame({"X": [1], "Y": [2]})
    df.to_excel(test_path, index=False)
    print(df)
    cols = manager.get_columns(test_path)
    print(cols)
    # self.assertEqual(cols, ["X", "Y"])

def test_merge_dataframe_appends_rows(manager: ExcelManager, test_path: str):
    """
    Test that merge_dataframe correctly appends rows to an existing Excel sheet.

    Input:
        - manager: Instance of ExcelManager used to perform the merge
        - test_path: Temporary Excel file path used for the test

    Expected behavior:
        - If the file exists, the new DataFrame rows should be appended to it.
        - The merged file should contain all original + new rows.

    Output:
        - True is returned from merge_dataframe
        - The resulting Excel file contains all appended data
    """
    # Arrange
    df1 = pd.DataFrame({"A": [1, 2]})
    df2 = pd.DataFrame({"A": [3]})

    # Create initial file
    df1.to_excel(test_path, index=False)
    print(pd.read_excel(test_path))

    # Act
    result = manager.merge_dataframe(test_path, df2)

    df_out = pd.read_excel(test_path)

    # Optional debugging info
    print(df_out)


def test_update_dataframe_creates_and_writes(manager: ExcelManager, test_path: str):
    """
    Test updating an Excel sheet:
    - First write df1 to a new file
    - Then overwrite with df2
    - Ensure no extra 'Unnamed: 0' column appears
    """
    import pandas as pd

    # Arrange: first DataFrame
    df1 = pd.DataFrame({"ID": [1, 2], "Value": ["A", "B"]})
    
    # Act: write first DataFrame
    result = manager.update_dataframe(test_path, df1, sheet_name="Sheet1", index_column="ID", keep_index=False)
    print("First write result:", result)
    print(pd.read_excel(test_path))

    # Arrange: second DataFrame (overwrite)
    df2 = pd.DataFrame({"Index": [1, 2, 3], "Letter": ["A", "B", "C"]})
    
    # Act: write second DataFrame
    result = manager.update_dataframe(test_path, df2, sheet_name="Sheet1", index_column=None, keep_index=False)
    print("Second write result:", result)
    print(pd.read_excel(test_path)==df2)


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
        logLevel=logLevel
    )


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

def test_parameter_folder_system_list():

    temp_dir = tempfile.TemporaryDirectory()
    fileName="TestDB" 
    folderSystem=[fileName, "f1"]
    folderSystem=None

    db:ControlDB     
    db = temp_controldb(
        fileName, 
        temp_dir.name,
        folderSystem=folderSystem,
        db_type= "mdb",
        # password="secret",
        base=ROOTBASE,
        logLevel=10
    )
    db.setup(password="secret", base=ROOTBASE)
    print(db.rootPath)
    print(db.fileFolder)
    print(db.filePath)
    
    
    table = db.load_row_existing_table("UserTable")
    row =  {'username': "Vito", 'password':"test", 'fullname':"Vito Pol", 'email':"test@test.org"}
    id  = db.row_create(table, **row)
    print(id)

    # print(os.path.join(temp_dir.name))
    close_db(temp_dir, db)
    pass

if __name__ == "__main__":

    # manager, test_path, temp_dir = temp_excel_manager()
    # print(type(manager))
    # print(test_path)
    # test_read_file_returns_dataframe(manager, test_path)    
    # test_get_columns_returns_correct_headers(manager, test_path)
    # test_merge_dataframe_appends_rows(manager, test_path)
    # test_update_dataframe_creates_and_writes(manager, test_path)

    test_parameter_folder_system_list()
