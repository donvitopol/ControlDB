import unittest
import os
import tempfile
from  src.utils import UtilsTable, UtilsRow, require_authorization
from src import ControlDB, ROOTBASE, UserTable
from tests.utils import temp_controldb, close_db, create_dummy_dataframe, make_temp_excel, temp_excel_manager, safe_remove_folder
from sqlalchemy import Engine, Table

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
    # Test TestUtils
    # -----------------------

    def create_test(self, table:UtilsTable, data:dict):
        print(f" -- merge_test")     
        
        # Create row
        ref_id = table.get_first_free_id()
        id  = table.row.create(**data)
        self.assertEqual(ref_id, id)
        row = table.row.get()
        
        print(f"data:           {data}")
        for key, value in data.items():
            # print(f"key:    {key}")
            # print(f"value:  {value}")
            self.assertEqual(value, row[key])

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

    def replace_test(self, table:UtilsTable, data:dict, id:int = None):   
        print(f" -- replace_test")     
        if id is not None:
            table.row.id = id

        result  = table.row.replace(data)
        # print(result)
        self.assertTrue(result)
        row = table.row.get()

        print(f"data:           {data}")
        print(f"row:            {row}")
        for key, value in data.items():
            self.assertEqual(value, row[key])
            
        extra_data = {k: v for k, v in row.items() if k not in data}
        print(f"extra_data:     {extra_data}")
        for key, value in extra_data.items():
            self.assertIsNone(row[key])

        # self.assertEqual(1, 2)

    
    def table_manipulate_test(self, table_identity: any, ref_is_core:bool, ref_column_names:list):   
        print(f" -- table_manipulate_test")   
        
        table = self.db.load_table(table_identity)
        self.assertIsInstance(table, UtilsTable)
        

        self.assertEqual(table.is_core, ref_is_core) 
        self.assertTrue(hasattr(table, "connect"))
        self.assertTrue(hasattr(table, "table_class")) 
        self.assertEqual(table.engine, self.db.engine)
        self.assertEqual(table.session, self.db.session)
        self.assertEqual(table.base, self.db.base) 

    
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



        # self.assertEqual(1, 2)
    
    
    
    def test_utils_core_load_table(self):
        print(f" -- test_utils_table_row_core_load_manipulate")     
        ref_column_names = ['ID', 'username', 'password', 'fullname', 'email']
        self.table_manipulate_test("UserTable", True, ref_column_names)



    def test_utils_org_load_table(self):
        print(f" -- test_utils_table_row_core_load_manipulate")  
        ref_column_names = ['ID', 'username', 'password', 'fullname', 'email']
        self.table_manipulate_test(UserTable, False, ref_column_names)


        

        # self.assertEqual(1, 2)


    # def test_utilmanager_orm_table_row_create(self):
    #     row =  {'username': "Vito", 'password':self.password, 'fullname':"Vito Pol", 'email':"test@test.org"}
    #     id  = self.db.row_create(UserTable, **row)
    #     self.assertEqual(1, id)
    #     _row = self.db.get.row(UserTable, id)
    #     self.assertEqual("Vito", _row["username"])
    #     # self.assertTrue(False)
    #     pass

    # def test_utilmanager_row_create(self):
    #     pass
    # # -----------------------
    # # Test TestUtilGetManager
    # # -----------------------

    # def test_utilgetmanager(self):

    #     pass



if __name__ == "__main__":
    unittest.main()
