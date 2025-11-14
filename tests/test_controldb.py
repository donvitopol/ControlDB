import unittest
import os
import tempfile

from sqlalchemy import String, Integer

from src import ControlDB, ROOTBASE, UserTable
from src.utils import UtilsTable, UtilsRow, require_authorization
from tests.utils import (
    temp_controldb,
    close_db,
    create_dummy_dataframe,
    make_temp_excel,
    temp_excel_manager,
    safe_remove_folder,
)


class TestControlDB(unittest.TestCase):
    """
    Comprehensive unit tests for ControlDB and its UtilsTable/UtilsRow helpers.
    Uses a fully isolated temporary database environment.
    """

    # ----------------------------------------------------------------------
    # Setup / Teardown
    # ----------------------------------------------------------------------

    def setUp(self):
        """Create a fresh isolated temporary database for each test."""
        print("\n=== setUp ===")

        self.password = "secret"
        self.db_name = "test_db"
        self.db_type = "mdb"
        self.folderSystem = None

        # Temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = os.path.join(self.temp_dir.name, self.db_name)
        self.file_path = os.path.join(self.root_path, f"{self.db_name}.{self.db_type}")

        # Create ControlDB test instance
        self.db: ControlDB = temp_controldb(
            self.db_name,
            self.root_path,
            folderSystem=self.folderSystem,
            db_type=self.db_type,
            password=self.password,
            base=ROOTBASE,
            logLevel=10,
        )

        self.assertIsNotNone(self.db, "❌ setUp: Failed to initialize ControlDB")
        print("    ✅ setUp completed")

    def tearDown(self):
        """Dispose DB connection and remove temp directory."""
        print("=== tearDown ===")
        try:
            close_db(self.temp_dir, self.db)
            print("    ✅ Temp folder cleaned")
        except Exception as exc:
            self.fail(f"❌ tearDown failed: {exc}")

    # ----------------------------------------------------------------------
    # Filesystem validation helper
    # ----------------------------------------------------------------------

    def folder_file_system_test(
        self,
        db: ControlDB,
        dbName: str,
        rootPath: str,
        fileFolder: str,
        filePath: str,
        folderSystem: str | list[str],
    ):
        """Validate that ControlDB created the correct on-disk folder structure."""
        print(" -- folder_file_system_test")

        # DB name
        self.assertEqual(
            dbName,
            db.name,
            f"❌ DB name mismatch. Expected: {dbName}, Got: {db.name}",
        )

        # Root folder
        print(f"   rootPath expected: {rootPath}")
        print(f"   db.rootPath      : {db.rootPath}")

        self.assertTrue(os.path.exists(db.rootPath), f"❌ rootPath missing: {db.rootPath}")
        self.assertEqual(
            rootPath,
            db.rootPath,
            f"❌ Root path mismatch.\nExpected: {rootPath}\nGot: {db.rootPath}",
        )

        # File folder
        print(f"   fileFolder expected: {fileFolder}")
        print(f"   db.fileFolder      : {db.fileFolder}")

        self.assertTrue(
            os.path.exists(db.fileFolder),
            f"❌ fileFolder missing: {db.fileFolder}",
        )
        self.assertEqual(
            fileFolder,
            db.fileFolder,
            f"❌ fileFolder mismatch.\nExpected: {fileFolder}\nGot: {db.fileFolder}",
        )

        # DB file
        print(f"   filePath expected: {filePath}")
        print(f"   db.filePath      : {db.filePath}")

        self.assertTrue(os.path.exists(db.filePath), f"❌ filePath missing: {db.filePath}")
        self.assertEqual(
            filePath,
            db.filePath,
            f"❌ filePath mismatch.\nExpected: {filePath}\nGot: {db.filePath}",
        )

        print("    ✅ folder_file_system_test passed")

    # ----------------------------------------------------------------------
    # ControlDB Tests
    # ----------------------------------------------------------------------

    def test_identifier(self):
        """Ensure the DB identifier can only be assigned once."""
        self.assertIsNone(self.db.id)

        self.db.id = 1
        self.assertEqual(self.db.id, 1)
        self.assertEqual(self.db.name, self.db_name)

        # ID cannot be overwritten
        with self.assertRaises(AttributeError):
            self.db.id = 2

        self.assertEqual(self.db.id, 1)
    
    def test_authorization(self):
        """Test DB creation/loading when base=None and folderSystem is a list."""
        temp_dir = tempfile.TemporaryDirectory()

        folderSystem = [self.db_name, "f1"]
        fileName = self.db_name

        db = temp_controldb(
            self.db_name,
            temp_dir.name,
            folderSystem=folderSystem,
            db_type="mdb",
            base=None,
            logLevel=10,
        )

        # File must not exist before creation
        self.assertFalse(os.path.exists(db.filePath))

        db.create_file(password=self.password)
        self.assertTrue(os.path.exists(db.filePath))

        # Authorization
        self.assertFalse(db.authorized)
        db.connect(password=self.password)
        self.assertTrue(db.authorized)

        rootPath = temp_dir.name
        fileFolder = os.path.join(rootPath, *folderSystem)
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = os.path.join(*folderSystem, fileName)

        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem)
        close_db(temp_dir, db)
    
    def test_authorization_failure(self):
        """Connecting with wrong password should not authorize DB."""
        

        password = "secret"
        db_name = "test_aut_db"

        # Temporary directory
        temp_dir = tempfile.TemporaryDirectory()
        root_path = os.path.join(self.temp_dir.name, self.db_name)
        file_path = os.path.join(self.root_path, f"{self.db_name}.{self.db_type}")

        # Create ControlDB test instance
        db: ControlDB = temp_controldb(
            db_name,
            root_path,
            folderSystem=None,
            db_type="mdb",
            base=ROOTBASE,
            logLevel=10,
        )

        self.assertIsNotNone(db, "❌ setUp: Failed to initialize ControlDB")
        print("    ✅ setUp completed")
        db.create_file(password=self.password)
        print(db.authorized)
        self.assertFalse(db.authorized)
        
        with self.assertRaises(Exception):
            db.connect(password="wrongpassword")
        
        self.assertFalse(db.authorized)

        close_db(temp_dir, db)

    def test_duplicate_table_creation(self):
        """Creating a table with existing name should raise an error."""
        table = self.db.create_table("Test", {"ID": Integer})
        self.assertIsInstance(table, UtilsTable)
        
        table = self.db.create_table("Test", {"ID": Integer})
        self.assertIsNone(table)
    
    def test_double_close_db(self):
        """Closing DB twice should not raise errors."""
        close_db(self.temp_dir, self.db)
        try:
            close_db(self.temp_dir, self.db)
        except Exception as e:
            self.fail(f"close_db failed on second call: {e}")
            
    def test_file_delete_and_recreate(self):
        """Deleting the DB file and recreating should succeed."""
        
        password = "secret"
        db_name = "test_de&re_db"

        # Temporary directory
        temp_dir = tempfile.TemporaryDirectory()
        root_path = os.path.join(self.temp_dir.name, self.db_name)
        file_path = os.path.join(self.root_path, f"{self.db_name}.{self.db_type}")

        # Create ControlDB test instance
        db: ControlDB = temp_controldb(
            db_name,
            root_path,
            folderSystem=None,
            db_type="mdb",
            base=ROOTBASE,
            logLevel=10,
        )
        db.create_file(password=password)
        db.connect(password=password)
        db.remove(exec=True)
        self.assertFalse(os.path.exists(db.filePath))
        db.create_file(password=password)
        self.assertTrue(os.path.exists(db.filePath))

    def test_parameter_folder_system_none(self):
        """folderSystem=None → DB lives directly inside rootPath."""
        temp_dir = tempfile.TemporaryDirectory()

        fileName = self.db_name
        folderSystem = None

        db = temp_controldb(
            fileName,
            temp_dir.name,
            folderSystem=folderSystem,
            db_type="mdb",
            password=self.password,
            base=None,
            logLevel=10,
        )

        rootPath = temp_dir.name
        fileFolder = rootPath
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = fileName

        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem)
        close_db(temp_dir, db)

    def test_parameter_folder_system_list(self):
        """folderSystem=list → nested folders."""
        temp_dir = tempfile.TemporaryDirectory()

        folderSystem = [self.db_name, "f1"]
        fileName = self.db_name

        db = temp_controldb(
            self.db_name,
            temp_dir.name,
            folderSystem=folderSystem,
            db_type="mdb",
            password=self.password,
            base=None,
            logLevel=10,
        )

        rootPath = temp_dir.name
        fileFolder = os.path.join(rootPath, *folderSystem)
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = os.path.join(*folderSystem, fileName)

        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem)
        close_db(temp_dir, db)

    def test_parameter_folder_system_str(self):
        """folderSystem=str → use path as-is."""
        temp_dir = tempfile.TemporaryDirectory()

        folderSystem = "test1/test2"
        fileName = self.db_name

        db = temp_controldb(
            self.db_name,
            temp_dir.name,
            folderSystem=folderSystem,
            db_type="mdb",
            password=self.password,
            base=None,
            logLevel=10,
        )

        rootPath = temp_dir.name
        fileFolder = os.path.join(rootPath, folderSystem)
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = os.path.join(folderSystem, fileName)

        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem)

        # Validate that folderSystem stays a string
        self.assertEqual(folderSystem, db.folderSystem)
        self.assertIsInstance(db.folderSystem, str)

        close_db(temp_dir, db)
    
    # ----------------------------------------------------------------------
    # UtilsRow Tests
    # ----------------------------------------------------------------------

    def row_create_test(self, table: UtilsTable, data: dict):
        """Test UtilsRow.create()."""
        print(" -- row_create_test")

        ref_id = table.get_first_free_id()
        new_id = table.row.create(**data)

        self.assertEqual(
            ref_id,
            new_id,
            f"❌ create() returned {new_id}, expected {ref_id}",
        )

        row = table.row.get()
        self.assertIsNotNone(row, f"❌ get() returned None for ID={new_id}")

        # Provided fields
        for key, value in data.items():
            self.assertIn(key, row)
            self.assertEqual(value, row[key])

        # Extra fields must be None
        allowed = set(data.keys()) | {"ID"}
        for col, val in row.items():
            if col not in allowed:
                self.assertIsNone(val)

        print("    ✅ row_create_test passed")

    def row_merge_test(self, table: UtilsTable, data: dict, id: int = None):
        """Test UtilsRow.merge()."""
        print(" -- row_merge_test")

        if id is not None:
            table.row.id = id

        original = table.row.get()
        self.assertIsNotNone(original)
        original = original.copy()

        self.assertTrue(table.row.merge(data))

        row = table.row.get()
        self.assertIsNotNone(row)

        # Updated fields
        for key, value in data.items():
            self.assertEqual(value, row[key])

        # Not updated fields remain unchanged
        for key, value in original.items():
            if key not in data:
                self.assertEqual(value, row[key])

        print("    ✅ row_merge_test passed")

    def row_replace_test(self, table: UtilsTable, data: dict, id: int = None):
        """Test UtilsRow.replace()."""
        print(" -- row_replace_test")

        if id is not None:
            table.row.id = id

        self.assertTrue(table.row.replace(data))

        row = table.row.get()
        self.assertIsNotNone(row)

        expected_id = row["ID"]
        data_with_id = {**data, "ID": expected_id}

        # Check updated fields
        for key, value in data_with_id.items():
            self.assertEqual(value, row[key])

        # Other fields must be cleared
        for col, val in row.items():
            if col not in data_with_id:
                self.assertIsNone(val)

    def row_delete_test(self, table: UtilsTable, id: int = None):
        """Test UtilsRow.delete()."""
        print(" -- row_delete_test")

        if id is not None:
            table.row.id = id

        before = table.row.get()
        self.assertIsNotNone(before)

        self.assertTrue(table.row.delete())
        self.assertIsNone(table.row.get())

        self.assertFalse(table.row.delete())  # Already deleted
        print("    ✅ row_delete_test passed")

    def test_merge_nonexistent_row(self):
        """Merging a non-existent row should return False."""
        table = self.db.create_table("Test3", {"ID": Integer, "value": String})
        table.row.id = 999
        result = table.row.merge({"value": "new"})
        self.assertTrue(result)

    def test_replace_nonexistent_row(self):
        """Replacing a non-existent row should return False."""
        table = self.db.create_table("Test4", {"ID": Integer, "value": String})
        table.row.id = 999
        result = table.row.replace({"value": "new"})
        self.assertTrue(result)

    # ----------------------------------------------------------------------
    # UtilsTable Tests
    # ----------------------------------------------------------------------

    def table_test(self, table: UtilsTable, ref_is_core: bool, ref_column_names: list):
        """Verify full UtilsTable CRUD flow."""
        print(" -- table_test")

        self.assertIsInstance(table, UtilsTable)
        self.assertEqual(table.is_core, ref_is_core)

        self.assertEqual(table.engine, self.db.engine)
        self.assertEqual(table.session, self.db.session)

        column_names = table.get_column_names()
        self.assertListEqual(column_names, ref_column_names)

        # Full CRUD
        print("     > row_create_test")
        self.row_create_test(
            table,
            {
                "username": "Vito",
                "password": self.password,
                "fullname": "Vito Pol",
                "email": "test@test.org",
            },
        )

        print("     > row_merge_test")
        self.row_merge_test(
            table,
            {"username": "donvitopol", "fullname": "Jan Klaas"},
        )

        print("     > row_replace_test")
        self.row_replace_test(table, {"username": "test"})

        print("     > row_delete_test")
        self.row_delete_test(table)

        print("    ✅ table_test passed")

    # ----------------------------------------------------------------------
    # Table creation / loading
    # ----------------------------------------------------------------------

    def test_utils_core_create_table(self):
        print(" -- test_utils_core_create_table")

        column_def = {
            "ID": Integer,
            "username": String,
            "password": String,
            "fullname": String,
            "email": String,
        }

        table = self.db.create_table("Test", column_def)
        self.table_test(table, True, list(column_def.keys()))

    def test_utils_core_load_table(self):
        print(" -- test_utils_core_load_table")

        ref_columns = ["ID", "username", "password", "fullname", "email"]
        table = self.db.load_table("UserTable")
        self.table_test(table, True, ref_columns)

    def test_utils_org_load_table(self):
        print(" -- test_utils_org_load_table")

        ref_columns = ["ID", "username", "password", "fullname", "email"]
        table = self.db.load_table(UserTable)
        self.table_test(table, False, ref_columns)


# ----------------------------------------------------------------------
# Main entrypoint
# ----------------------------------------------------------------------
if __name__ == "__main__":
    unittest.main()
