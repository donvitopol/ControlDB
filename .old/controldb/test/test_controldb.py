import unittest
from datetime import datetime, date
import os, sys
from sqlalchemy import Engine
import shutil 
# from dotenv import load_dotenv

sourcePath =  os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
sys.path.append(sourcePath)
from pretty_logger import prettylog

if __name__ == "main":
    from test_data import *
from controldb import ControlDB, ControlDBGet

# load_dotenv()

######################################################################
###               Create references For The Test.                 ###
######################################################################
 
class TestControlDBReff():
    def __init__(self) -> None:
        pass
    """
    def create_reff_controldb(self, mainPath, **kwargs):  
        print("\n\n  - create_reff_controldb") 
        # print(f"     -> kwargs:", kwargs)  

        self.mainPath = mainPath
        if("dbName" in kwargs.keys()):
            self.dbName = kwargs["dbName"]  
        else:
            self.dbName = 'root'
        # print("     => Ref dbName:      ", self.dbName)

        if("dirName" in kwargs.keys()): 
            dirName = kwargs["dirName"]
        else:
            dirName = self.dbName
        self.dbDir = os.path.join(mainPath, dirName)
        # print("     => Ref dbDir:       ", self.dbDir)

        if("fileType" in kwargs.keys()):   
            self.fileType = kwargs["fileType"]
        else:
            self.fileType = 'mdb'

        if("fileName" in kwargs.keys()): 
            self.fileName = kwargs["fileName"]
        else:
            self.fileName = self.dbName
        # print("     => Ref fileName:    ", self.fileName)

        self.dbFile = os.path.join(self.dbDir, f"{self.fileName}.{self.fileType}")

        self.ldbFile = os.path.join(self.dbDir, f"{self.fileName}.ldb")
        # print("     => Ref dbFile:      ", self.dbFile)
        self.mBuDir = os.path.join(self.mainPath, "backup") 

        if("fileName" in kwargs.keys()): 
            self.fileName = kwargs["fileName"]
        else:
            self.fileName = self.dbName  
        
        self.backupName = f"backup_{self.dbName}".replace(" ", "_").lower() 
        # print("     => Ref backupName:    ", self.backupName)
        if("buDir" in kwargs.keys()):  
            self.mBuDir = kwargs["buDir"] 
            self.buDir = os.path.join(self.mBuDir, self.backupName)         
        else:
            self.mBuDir = os.path.join(self.mainPath, "backup")
            self.buDir = os.path.join(self.mBuDir, self.backupName) 
        # print("     => Ref mBuDir:      ", self.mBuDir)
        # print("     => Ref buDir:       ", self.buDir)

        if("logLevel" in kwargs.keys()):   
            self.logLevel = kwargs["logLevel"]
        else:
            self.logLevel = 30


        # Initialize database controller
        # cls.db = ControlDB(cls.dbName, cls.mainPath, dirName=cls.dirName, fileName=fileName, buDir=cls.buDir, logLevel=40)
        self.db = ControlDB(mainPath, **kwargs)
        engine = self.db.connect()
        print (f" => engine:      ", engine)
        print (f" => mainPath:      ", mainPath)
        print (f" => kwargs:        ", kwargs)
        return engine, mainPath, kwargs
    """

######################################################################
###                            Main Test.                          ###
######################################################################
           
class TestControlDB(unittest.TestCase, TestControlDBReff):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        TestControlDBReff.__init__(self)

    # Infokes Ones at start executed
    @classmethod
    def setUpClass(cls):
        print('\nsetUpClass\n')
        cls.maxIndexInitTests = 4
        cls.mainPath =  os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))))  
        # # Auto parametrization of class variables
        # cls.dbName = cls.dirName = cls.fileName = "root"
        # cls.mBuDir = os.path.join(cls.mainPath, "backup")  
        # backupName = f"backup_{cls.dbName}".replace(" ", "_").lower()
        # cls.buDir = os.path.join(cls.mBuDir, backupName)  
        # cls.fileType = "mdb"
        # cls.db = ControlDB(cls.mainPath)

        rootDir =  os.path.join(cls.mainPath, "root")
        if os.path.isdir(rootDir):
            shutil.rmtree(rootDir)
        buDir =  os.path.join(cls.mainPath, "backup")
        if os.path.isdir(buDir):
            shutil.rmtree(buDir)
    
    # Infokes Ones at end executed
    # @unittest.skip
    @classmethod
    def tearDownClass(cls):
        print('\ntearDownClass')
        buDir =  os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))), "backup")
        if os.path.isdir(buDir):
            shutil.rmtree(buDir)
        # if os.path.isdir(cls.dbDir):
        #     shutil.rmtree(cls.dbDir)
        # pass

    def tearDown(self):
        print('\ntearDown')
        # if os.path.isdir(self.dbDir):
        #     shutil.rmtree(self.dbDir)
        # if os.path.isdir(self.mBuDir):
        #     shutil.rmtree(self.mBuDir)
        # pass
    
    def runTest(self):
        self.init_cases()
        # self.init_cases(1)
        # self.setup_test(1)
        # self.control_table_test(1)



        # self.test_setup()
        # self.backup_manual_test()
        # self.init_test()
        # self.create_test()
        # self.connect_test()
        # self.backup_test()

        # self.remove_test()
        pass


    ######################################################################
    ###               Test chain events of the object.                 ###
    ######################################################################
 
    def init_cases(self, case:int=None):
        # Auto initialization
        if case == 0 or case is None:
            self.create_reff_controldb(self.mainPath)
            self.init_test("Auto Test")

        if case == 1 or case is None:
            self.create_reff_controldb(self.mainPath, logLevel=10)
            self.init_test("Manual logLevel")

        if case == 2 or case is None:
            self.create_reff_controldb(self.mainPath, dbName="Manual dbName", logLevel=10)
            self.init_test("Manual dbName")

        if case == 3 or case is None:
            self.create_reff_controldb(self.mainPath, dbName="Manual dbName", dirName="database", logLevel=10)
            self.init_test("Manual dirName")

        if case == 4 or case is None:
            self.create_reff_controldb(self.mainPath, dbName="Manual dbName", dirName="database", fileName="USP", logLevel=10)
            self.init_test("Manual fileName")
            
        pass
     
    def test_setup(self):
        for x in range(self.maxIndexInitTests):
            self.setup_test(x)

    def setup_test(self, initIndex):
            self.init_cases(initIndex)        
            self.create_test()        
            self.connect_test()  
            self.backup_test()          
            self.engine_test() 
            self.remove_test()   

    def control_table_test(self, case:int):
        self.init_cases(case)   
        self.create_test()   
        self.connect_test()          

        columns =  TESTTABLE1
        data =  TESTRECORD1
        print(columns)
            
        refTableName:list = []   
        refColumnName:list = columns.keys()

        cicle:list = range(1)
        for c in cicle:
            if c == 0:
                n = "Test"
            else:
                n = "Test"+str(c)
            self.append_table_test(n, columns, refTableName)
            self.append_record_test(n, data, refColumnName)

        for c in reversed(cicle):
            if c == 0:
                n = "Test"
            else:
                n = "Test"+str(c)
            self.remove_table_test(n, refTableName)
  
        self.remove_test()


    ######################################################################
    ###               Test properties of the object.                   ###
    ######################################################################

    def engine_test(self):
        engine = self.db.engine  
        self.assertEqual(type(engine), Engine,                      "Test if engine is initized.")  
        self.assertTrue(self.db.connected,                          "Test connected after connection.")  
        db_1 = ControlDB(self.mainPath)
        self.assertFalse(db_1.connected,                            "Test connected after connection.")  
        db_1.engine = engine
        self.assertTrue(db_1.connected,                             "Test connected after connection.")  
        self.assertTrue(hasattr(self.db, 'get'),                    "Test if get has attribute.")  
        self.assertTrue("ControlDBGet" in str(type(db_1.get)),      "Test if ControlDBGet is initized.")  
       

    ######################################################################
    ###               Test functions of the object.                    ###
    ######################################################################

    def init_test(self, txt:str):
        print('init_test')      
        
        print (f" -> Ref mainPath:  ", self.mainPath)
        print (f" => mainPath:      ", self.mainPath)
        self.assertEqual(self.mainPath, str(self.db.mainPath),  f"Test {txt} -> Check database main path.") 

        print (f" -> Ref dbName:    ", self.dbName)
        print (f" => dbName:        ", self.db)
        self.assertEqual(self.dbName, str(self.db.name),             f"Test {txt} -> Check database name.")  
                  
        print (f" -> Ref dbDir:     ", self.dbDir)
        print (f" => dbDir:         ", self.db.dbDir)
        self.assertEqual(self.dbDir, self.db.dbDir,             f"Test {txt} -> Check database path: Directory path.")

        print (f" -> Ref dbFile:    ", self.dbFile)
        print (f" => dbFile:        ", self.db.dbFile)
        self.assertEqual(self.dbFile, self.db.dbFile,           f"Test {txt} -> Check database path: File path.")    

        print (f" -> Ref mBuDir:    ", self.mBuDir)
        print (f" => mBuDir:        ", self.db.mBuDir)
        self.assertEqual(self.mBuDir, self.db.mBuDir,           f"Test {txt} -> Check database path: Main backup directory.")  
        
        print (f" -> Ref buDir:     ", self.buDir)
        print (f" => buDir:         ", self.db.buDir)
        self.assertEqual(self.buDir, self.db.buDir,             f"Test {txt} -> Check database path: Backup directory.") 
        
        # self.assertTrue(hasattr(self.db, 'logger'),             f"Test {txt} -> Test if logger is initialized.")  
        # self.assertEqual(self.logLevel, self.db.logLevel,       f"Test {txt} -> Check database path: Backup directory.") 
        
    def create_test(self):
        print('create_test')  
        print (f" => dbDir:         ", self.db.dbDir)
        print (f" => dbFile:        ", self.db.dbFile)
        self.db.create()
        self.assertTrue(os.path.isdir(self.db.dbDir),       "Test if dir is created.")
        self.assertTrue(os.path.isfile(self.db.dbFile),     "Test if folder is created.")
     
    def connect_test(self):
        print('connect_test')
        self.assertFalse(self.db.connected,                 "Test connected before connection.")
        engine = self.db.connect()  
        self.assertTrue(hasattr(self.db, 'get'),            "Test if get has attribute.")  
        # self.assertEqual(type(self.db.get), ControlDBGet,   "Test if ControlDBGet is initialized.")  
        self.assertEqual(type(engine), Engine,              "Test if engine is initized.")  
        self.assertTrue(self.db.connected,                  "Test connected after connection.")  
        # self.ldb_closed_test()
    
    def backup_test(self):
        print('backup_test')

        buDir = self.db.backup() 
        t:datetime = datetime.now()  
        dirName = f"{date(t.year, t.month, t.day)} - {self.dbName}"
        backup = os.path.join(self.buDir, dirName)
        print (f" -> Ref return:     ", buDir)
        print (f" => backup:         ", backup)
        self.assertTrue(os.path.isdir(backup),              "Test if backup is created return value.")
        self.assertEqual(backup, buDir,                     "Test backup path of return value.") 
        # print(backup)

        mBuDir = os.path.join(self.mainPath, "backup")
        print (f" -> Ref mBuDir:     ", self.db.mBuDir)
        print (f" => mBuDir:         ", mBuDir)
        self.assertTrue(os.path.isdir(self.db.mBuDir),      "Test if main backup is created.")
        self.assertEqual(mBuDir, self.db.mBuDir,            "Test backup path.") 

        backupName = f"backup_{self.dbName}".replace(" ", "_").lower()
        buDir = os.path.join(mBuDir, backupName)
        print (f" -> Ref buDir:     ", buDir)
        print (f" => buDir:         ", self.db.buDir)
        self.assertTrue(os.path.isdir(buDir),               "Test if backup folder is created.")
        self.assertEqual(buDir, self.db.buDir,              "Test backup path.") 
               
    def remove_test(self):
        print('remove_test')      
        self.db.remove(exec=True)      
        self.assertFalse(os.path.isdir(self.db.dbDir),              "Test of dir is removed.")
        self.assertFalse(os.path.isfile(self.db.dbFile),            "Test of folder is removed.") 

    def append_table_test(self, tableName:str, columns:dict[str], ref:list[str]):
        print('append_table_test')   
        ref.append(tableName)
        self.db.append_table(tableName, columns)
        result = self.db.get.table_names()    
        self.assertEqual(result, ref,                               "Test if engine is initized.")  

    def append_record_test(self, tableName:str, data:dict, ref:list[str]):
        print('append_record_test')   
        self.db.append_record(tableName, data)
        result = self.db.get.column_names(tableName)     
        self.assertEqual(set(result), set(ref),                               "Test if engine is initized.")  

    def remove_table_test(self, tableName:str, ref:list[str]):
        print('remove_table_test')   
        self.db.remove_table(tableName)        
        ref.pop(-1)
        result = self.db.get.table_names()    
        self.assertEqual(result, ref,                               "Test if table is removed.")  

    ######################################################################
    ###               Test small occuring errors  .                    ###
    ######################################################################

    def ldb_opened_test(self):
        print('ldb_opened_test')
        self.assertTrue(self.db.connected,                  "Test connected after connection.")  
        self.assertEqual( self.ldbFile, self.db.ldbFile,    "Check path for file that doesn't want to close.")
        self.assertTrue(os.path.isdir(self.db.ldbFile),    "Test if file that prevents to remove is not present.")
        pass

    def ldb_closed_test(self):
        print('ldb_closed_test')
        self.assertTrue(self.db.connected,                  "Test connected after connection.")  
        self.assertEqual( self.ldbFile, self.db.ldbFile,    "Check path for file that doesn't want to close.")
        self.assertFalse(os.path.isdir(self.db.ldbFile),    "Test if file that prevents to remove is not present.")
        pass


 
        

        
if __name__ == "__main__":
    # unittest.main()
    
    suite = unittest.makeSuite(TestControlDB, 'Test ControlDB')
    # suite.addTests([TestMMultiplication(), TestAddition(), TestDivition()])
                        
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print('Error: ', [x[0] for x in result.errors])
    print('\nFailures: ', [x[-1] for x in result.failures])
    print('\nSkipped Tests: ', result.skipped)
    print('\nNo. of Tests: ', result.testsRun)
    print('\nWas is a succesful test? ', result.wasSuccessful())
    # unittest.main()