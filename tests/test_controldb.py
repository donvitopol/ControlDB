import unittest
import os
import tempfile
from  src.utils import UtilsTable, UtilsRow, require_authorization
from src import ControlDB, ROOTBASE, UserTable
from tests.utils import temp_controldb, close_db, create_dummy_dataframe, make_temp_excel, temp_excel_manager, safe_remove_folder
from sqlalchemy import Engine, Table, String, Integer

class TestControlDB(unittest.TestCase):
    """Unit tests for ControlDB using a real temporary database."""

    def setUp(self):
        """Create a temporary database folder and ControlDB instance."""
        self.password="secret"
        self.db_name = "test_db"  
        self.folderSystem=None
        self.db_type="mdb"

        self.temp_dir = tempfile.TemporaryDirectory()    
        self.root_path = os.path.join(self.temp_dir.name, self.db_name)
        self.file_path = os.path.join(self.root_path, f"{self.db_name}.{self.db_type}")

        self.db:ControlDB
        self.db = temp_controldb(
            self.db_name, 
            self.root_path,
            folderSystem=self.folderSystem,
            db_type= self.db_type,
            password=self.password,
            base=ROOTBASE,
            logLevel=10
        )

    def tearDown(self):
        """Clean up the temporary folder."""
        close_db(self.temp_dir, self.db)

    def folder_file_system_test(self, db:ControlDB, dbName:str, rootPath:str, fileFolder, filePath:str, folderSystem:str | list[str]):
        
        self.assertEqual(dbName, db.name)

        
        print("rootPath (test):     ", rootPath)
        print("db.rootPath:", db.rootPath)
        self.assertTrue(os.path.exists(db.rootPath))
        self.assertEqual(rootPath, db.rootPath)

        print("fileFolder (test):   ", fileFolder)
        print("db.fileFolder:", db.fileFolder)
        self.assertTrue(os.path.exists(db.fileFolder))
        self.assertEqual(fileFolder, db.fileFolder)

        print("filePath (test):     ", filePath)
        print("db.filePath:", db.filePath)
        self.assertTrue(os.path.exists(db.filePath))
        self.assertEqual(filePath, db.filePath)

    # -----------------------
    # Test ControlDB
    # -----------------------
    def test_identifier(self):
        """Test that the identifier can only be set once."""
        self.assertIsNone(self.db.id)
        self.db.id = 1
        self.assertEqual(1, self.db.id)
        self.assertEqual(self.db_name, self.db.name)

        # Assert that setting ID again raises an AttributeError
        with self.assertRaises(AttributeError):
            self.db.id = 2

        self.assertEqual(1, self.db.id)

    def test_connect_no_base(self):
        temp_dir = tempfile.TemporaryDirectory()
        fileName=self.db_name 
        folderSystem=[self.db_name, "f1"]

        db:ControlDB     
        db = temp_controldb(
            self.db_name, 
            temp_dir.name,
            folderSystem=folderSystem,
            db_type= "mdb",
            base=None,
            logLevel=10
        )

        self.assertFalse(os.path.exists(db.filePath))
        db.create_file(password=self.password)
        self.assertTrue(os.path.exists(db.filePath))

        self.assertFalse(db.authorized)
        db.connect(password=self.password)
        self.assertTrue(db.authorized)

        rootPath = os.path.join(temp_dir.name)
        fileFolder = os.path.join(rootPath, *folderSystem)
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = os.path.join(*folderSystem, fileName)
        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem=folderSystem)
        close_db(temp_dir, db)

    def test_parameter_folder_system_none(self):

        temp_dir = tempfile.TemporaryDirectory()  
        fileName=self.db_name 
        folderSystem=None

        db:ControlDB        
        db = temp_controldb(
            fileName, 
            os.path.join(temp_dir.name),
            folderSystem=folderSystem,
            db_type= "mdb",
            password=self.password,
            base=None,
            logLevel=10
        )

        rootPath = os.path.join(temp_dir.name)
        fileFolder = rootPath
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = fileName
        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem=folderSystem)
        close_db(temp_dir, db)
        pass

    def test_parameter_folder_system_list(self):

        temp_dir = tempfile.TemporaryDirectory()
        fileName=self.db_name 
        folderSystem=[self.db_name, "f1"]

        db:ControlDB     
        db = temp_controldb(
            self.db_name, 
            temp_dir.name,
            folderSystem=folderSystem,
            db_type= "mdb",
            password=self.password,
            base=None,
            logLevel=10
        )
        rootPath = os.path.join(temp_dir.name)
        fileFolder = os.path.join(rootPath, *folderSystem)
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = os.path.join(*folderSystem, fileName)
        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem=folderSystem)
        close_db(temp_dir, db)
        pass

    def test_parameter_folder_system_str(self):

        temp_dir = tempfile.TemporaryDirectory()
        fileName=self.db_name 
        folderSystem="test1/test2"

        db:ControlDB     
        db = temp_controldb(
            self.db_name, 
            temp_dir.name,
            folderSystem=folderSystem,
            db_type= "mdb",
            password=self.password,
            base=None,
            logLevel=10
        )
        rootPath = os.path.join(temp_dir.name)
        fileFolder = os.path.join(temp_dir.name, folderSystem)
        filePath = os.path.join(fileFolder, f"{self.db_name}.{self.db_type}")
        dbName = os.path.join(folderSystem, fileName)
        self.folder_file_system_test(db, dbName, rootPath, fileFolder, filePath, folderSystem=folderSystem)
        
        print("folderSystem (test):     ", folderSystem)
        print("db.folderSystem:", db.folderSystem)
        _folderSystem = os.path.join(*folderSystem) if isinstance(folderSystem, list) else folderSystem
        self.assertEqual(_folderSystem, db.folderSystem)
        if _folderSystem == None:
            self.assertEqual(type(db.folderSystem), None)
        else:
            self.assertEqual(type(db.folderSystem), str)
        
        
        close_db(temp_dir, db)
        pass

    # -----------------------
    # Test Utils UtilsTable and UtilsRow
    # -----------------------

    def create_test(self, table:UtilsTable, data:dict):
        print(" -- create_test")

        # --- 1. Get the first free ID and create the row ------------------------
        ref_id = table.get_first_free_id()
        new_id = table.row.create(**data)
        self.assertEqual(
            ref_id, new_id,
            f"❌ create() returned ID {new_id}, expected first free ID {ref_id}"
        )
        
        # --- 2. Fetch the row --------------------------------------------------
        row = table.row.get()
        self.assertIsNotNone(row, f"❌ get() returned None after create() for ID={new_id}")
        
        # --- 3. Validate all provided fields ----------------------------------
        for key, value in data.items():
            self.assertIn(key, row, f"❌ Column '{key}' missing in row after create()")
            self.assertEqual(
                value, row[key],
                f"❌ Column '{key}' value mismatch after create(): expected {value}, got {row[key]}"
            )

        # --- 4. Validate extra columns are None --------------------------------       
        data_with_id = {**data, "ID": new_id}
        extra_columns = {k: v for k, v in row.items() if k not in data_with_id}
        for col, val in extra_columns.items():
            self.assertIsNone(
                val,
                f"❌ Column '{col}' should be None after create(), but got {val}"
            )

        print("    ✅ create_test passed")
        # self.assertEqual(1, 2)
        
    def merge_test(self, table:UtilsTable, data:dict, id:int = None):
        print(f" -- merge_test")     
        
        if id is not None:
            table.row.id = id

        _row  = table.row.get().copy()

        result  = table.row.merge(data)
        self.assertTrue(result)
        row = table.row.get()
        
        print(f"data:           {data}")
        for key, value in data.items():
            # print(f"key:    {key}")
            # print(f"value:  {value}")
            self.assertEqual(value, row[key])
        
        print(f"_row:           {_row}")
        print(f"row:            {row}")
        extra_data = {k: v for k, v in _row.items() if k not in data}
        print(f"extra_data:     {extra_data}")
        # print(extra_data)
        for key, value in extra_data.items():
            self.assertEqual(value, row[key])

        # self.assertEqual(1, 2)
    
    def replace_test(self, table: UtilsTable, data: dict, id: int = None):
        print(" -- replace_test")

        # Ensure we start on the correct row
        if id is not None:
            table.row.id = id

        # Execute replace() and ensure it returns True
        result = table.row.replace(data)
        self.assertTrue(result, f"❌ replace() returned False for ID={id or table.row.id}")

        # Fetch the updated row
        row = table.row.get()
        self.assertIsNotNone(row, f"❌ get() returned None after replace() on ID={id}")

        # Ensure ID is preserved
        expected_id = row["ID"]
        data_with_id = {**data, "ID": expected_id}

        print(f"data_with_id:   {data_with_id}")
        print(f"row:            {row}")

        # Check that all provided fields match
        for key, value in data_with_id.items():
            self.assertIn(key, row, f"❌ Column '{key}' missing in row after replace()")
            self.assertEqual(
                value, row[key],
                f"❌ Column '{key}' value mismatch after replace(): expected {value}, got {row[key]}"
            )

        # Check all other columns were set to None
        extra_columns = {k: v for k, v in row.items() if k not in data_with_id}

        print(f"extra_columns:  {extra_columns}")

        for col, val in extra_columns.items():
            self.assertIsNone(
                val,
                f"❌ Column '{col}' should be None after replace(), but got {val}"
            )

        # self.assertEqual(1, 2)
    
    def delete_test(self, table: UtilsTable, id: int = None):
        print(" -- delete_test")

        # If an ID is provided, set it
        if id is not None:
            table.row.id = id

        # --- 1. VERIFY ROW EXISTS BEFORE DELETE ---------------------------------
        before_row = table.row.get()
        self.assertIsNotNone(
            before_row,
            f"❌ delete_test: Row with ID={table.row.id} does NOT exist before deletion!"
        )

        # --- 2. PERFORM DELETE ---------------------------------------------------
        result = table.row.delete()
        self.assertTrue(
            result,
            f"❌ delete_test: delete() returned False for ID={table.row.id}"
        )

        # --- 3. VERIFY ROW IS NOW GONE ------------------------------------------
        after_row = table.row.get()
        self.assertIsNone(
            after_row,
            f"❌ delete_test: Row with ID={table.row.id} STILL EXISTS after deletion!"
        )

        # --- 4. OPTIONAL: CHECK THAT DELETING AGAIN RETURNS FALSE ----------------
        second_try = table.row.delete()
        self.assertFalse(
            second_try,
            "❌ delete_test: delete() should return False when deleting a non-existing row!"
        )

        print("    ✅ delete_test passed")

    def table_manipulate_test(self, table: UtilsTable, ref_is_core:bool, ref_column_names:list):   
        print(f" -- table_manipulate_test")   
        
        self.assertIsInstance(table, UtilsTable)        

        self.assertEqual(table.is_core, ref_is_core) 
        self.assertTrue(hasattr(table, "connect"))
        self.assertTrue(hasattr(table, "table_class")) 
        self.assertEqual(table.engine, self.db.engine)
        self.assertEqual(table.session, self.db.session)

    
        columnNames = table.get_column_names()
        self.assertTrue(columnNames, ref_column_names)
        
        data =  {
            'username': "Vito", 
            'password':self.password, 
            'fullname':"Vito Pol", 
            'email':"test@test.org"
            }
        self.create_test(table, data)
        self.merge_test(table, {'username': "donvitopol", 'fullname':"Jan Klaas"})
        self.replace_test(table, {'username': "test"})
        # self.delete_test(table)

        # print(table.get_column_definitions())

        # self.assertEqual(1, 2)
    


    def test_utils_core_create_table(self):
        print(f" -- test_utils_table_row_core_load_manipulate")     

        column_def = {
            'ID': Integer, 
            'username': String, 
            'password': String, 
            'fullname': String,     
            'email': String
            }
        table = self.db.create_table("Test", column_def)

        ref_column_names = ['ID', 'username', 'password', 'fullname', 'email']
        self.table_manipulate_test(table, True, ref_column_names)

    def test_utils_core_load_table(self):
        print(f" -- test_utils_table_row_core_load_manipulate")     
        ref_column_names = ['ID', 'username', 'password', 'fullname', 'email']
        table = self.db.load_table("UserTable")
        self.table_manipulate_test(table, True, ref_column_names)

    def test_utils_org_load_table(self):
        print(f" -- test_utils_table_row_core_load_manipulate")  
        ref_column_names = ['ID', 'username', 'password', 'fullname', 'email']
        table = self.db.load_table(UserTable)
        self.table_manipulate_test(table, False, ref_column_names)



if __name__ == "__main__":
    unittest.main()
