import unittest
import os
import tempfile
from src import ControlDB, UtilManager, UtilGetManager, ROOTBASE, UserTable
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
    # Test TestUtilManager
    # -----------------------
    def test_utilmanager_core_table_load_row_control(self):
        table = self.db.load_row_existing_table("UserTable")
        self.assertEqual(type(table), Table)
        columnNames = self.db.get.column_names(table)
        self.assertTrue(columnNames, ['ID', 'username', 'password', 'fullname', 'email'])
        row =  {'username': "Vito", 'password':self.password, 'fullname':"Vito Pol", 'email':"test@test.org"}
        
        # Create row
        id  = self.db.row_create(table, **row)
        self.assertEqual(1, id)
        _row = self.db.get.row(table, id)
        self.assertEqual("Vito", _row["username"])


        result  = self.db.row_merge(table, id, {'username': "donvitopol"})
        self.assertTrue(result)
        _row = self.db.get.row(table, id)
        self.assertEqual("donvitopol", _row["username"])
        self.assertEqual(self.password, _row["password"])

        result  = self.db.row_replace(table, id, {'username': "test"})
        self.assertTrue(result)


    def test_utilmanager_orm_table_row_create(self):
        row =  {'username': "Vito", 'password':self.password, 'fullname':"Vito Pol", 'email':"test@test.org"}
        id  = self.db.row_create(UserTable, **row)
        self.assertEqual(1, id)
        _row = self.db.get.row(UserTable, id)
        self.assertEqual("Vito", _row["username"])
        # self.assertTrue(False)
        pass

    def test_utilmanager_row_create(self):
        pass
    # -----------------------
    # Test TestUtilGetManager
    # -----------------------

    def test_utilgetmanager(self):

        pass










    # def test_create_file_creates_file(self):
    #     file_name = "new_db"
    #     new_db = ControlDB(fileName=file_name, rootPath=self.temp_dir.name)
    #     created = new_db.create_file(password="secret")
    #     self.assertTrue(created)
    #     self.assertTrue(os.path.exists(new_db.filePath))
    #     new_db.detach()

    # def test_remove_folder_successful(self):
    #     """Test that remove_folder deletes the folder after the database file is removed."""
    #     # Stap 1: verwijder de databasefile
    #     result_file = self.db.remove(exec=True, removeFolder=False)
    #     self.assertTrue(result_file)
    #     self.assertFalse(os.path.exists(self.db.filePath))

    #     # Stap 2: verwijder de folder via helper
    #     result_folder = safe_remove_folder(self.db)
    #     self.assertTrue(result_folder)
    #     self.assertFalse(os.path.exists(self.db.rootPath))

    # def test_remove_folder_dryrun(self):
    #     result = self.db.remove_folder(exec=False)
    #     self.assertFalse(result)
    #     self.assertTrue(os.path.exists(self.db.rootPath))

    # def test_setup_creates_file_and_connects(self):
    #     file_name = "setup_db"
    #     new_db = ControlDB(fileName=file_name, rootPath=self.temp_dir.name)
    #     result = new_db.setup(password="secret")
    #     self.assertTrue(result)
    #     self.assertTrue(new_db.authorized)
    #     self.assertTrue(os.path.exists(new_db.filePath))
    #     new_db.detach()

    # def test_temp_excel_manager_integration(self):
    #     manager, test_path, temp_dir = temp_excel_manager()
    #     df_path = make_temp_excel(create_dummy_dataframe())
    #     self.assertTrue(os.path.exists(df_path))
    #     temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
