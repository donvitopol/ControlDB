import unittest
from datetime import datetime, date
import os, sys
from sqlalchemy import Engine
import shutil 

from pretty_logger import prettylog

sourcePath =  os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
sys.path.append(sourcePath)
print(sourcePath)
# import controldb
from utils import print_title
from controldb import ControlDBGet, ControlDB
from table import Table
from mydb import MyDB
from test_controldb import TestControlDBReff
from test_utils import TestUtils
from test_data import TESTTABLE1, TESTRECORD1, TESTDF1

######################################################################
###               Create references For The Test.                 ###
######################################################################
class TestMyDBReff(TestControlDBReff):
    def __init__(self) -> None:
        TestControlDBReff.__init__(self)

    def create_reff_mydb(self, mainPath, dbName, **kwargs):  
        print("  - create_reff_mydb") 
        print(f"     -> kwargs:", kwargs)  
        self.mainPath = mainPath
        print("     => Ref mainPath:        ", self.mainPath)

        self.dbName = dbName
        print("     => Ref dbName:          ", self.dbName)

        if("dirName" in kwargs.keys()): 
            dirName = kwargs["dirName"]
        else:
            dirName = self.dbName
        self.dirName = dirName
        print("     => Ref dirName:         ", self.dirName)
        self.dbDir = os.path.join(mainPath, dirName)
        print("     => Ref dbDir:           ", self.dbDir)

        if("fileType" in kwargs.keys()):   
            self.fileType = kwargs["fileType"]
        else:
            self.fileType = 'mdb'
        print("     => Ref fileType:       ", self.fileType)

        if("fileName" in kwargs.keys()): 
            self.fileName = kwargs["fileName"]
        else:
            self.fileName = "database"
        print("     => Ref fileName:    ", self.fileName)

        self.dbFile = os.path.join(self.dbDir, f"{self.fileName}.{self.fileType}")
        print("     => Ref dbFile:      ", self.dbFile)

        self.ldbFile = os.path.join(self.dbDir, f"{self.fileName}.ldb")
        print("     => Ref ldbFile:      ", self.ldbFile)

        
        self.backupName = "BU_"  + f"{self.dbName}".replace(" ", "_").lower()
        print("     => Ref backupName:   ", self.backupName)

        
        if("buDir" in kwargs.keys()):  
            self.mBuDir = kwargs["buDir"] 
            self.buDir = os.path.join(self.mBuDir, self.backupName)         
        else:
            self.mBuDir = os.path.join(self.mainPath, "backup")
            self.buDir = os.path.join(self.mBuDir, self.backupName) 
        print("     => Ref mBuDir:      ", self.mBuDir)
        print("     => Ref buDir:       ", self.buDir)


        t:datetime = datetime.now()  
        x = 0
        while True:
            if x == 0:
                    dirName = f"{date(t.year, t.month, t.day)} - {self.dbName}"
            else:
                    dirName = f"{date(t.year, t.month, t.day)} ({x}) - {self.dbName}"
            
            self.dDuDir =  os.path.join(self.buDir, dirName)
            if not os.path.isdir(self.dDuDir):  
                break
            x += 1   
        print("     => Ref dBuDir:       ", self.dDuDir)

        if("logLevel" in kwargs.keys()):   
            self.logLevel = kwargs["logLevel"]
        else:
            self.logLevel = 30

        self.db = MyDB(mainPath, dbName, **kwargs)
        print("\n")

    ######################################################################
    ###               Test chain events of the object.                 ###
    ######################################################################
    def init_cases_mydb(self, case:int=None, _print:bool = False):
        # Create database name and create folder
        x = 0
        while True:
            if x == 0:
                dbName = "test_database"
            else:
                dbName = f"test_database_{x}"

            dbdir = os.path.join(self.mainPath, dbName)
            if not os.path.isdir(dbdir):
                break
            x+=1

        x=0
        while True:
            dirName = f"database_{x}"
            dirPath = os.path.join(self.mainPath, dbName, dirName)
            if not (os.path.isdir(dirPath)):
                break
            x += 1

        logLevel=10 

        if case == 0 or case is None:
            ''' Auto initialization.            
            '''
            self.create_reff_mydb(self.mainPath, dbName)

        # Auto initialization
        if case == 1 or case is None:
            ''' Auto initialization with log.            
            '''
            self.create_reff_mydb(self.mainPath, dbName, logLevel=logLevel)

        if case == 2 or case is None:
            ''' Initialization 
                - with logger.   
                - with manual dir name.              
            '''
            self.create_reff_mydb(self.mainPath, dbName, logLevel=logLevel, dirName=dirName)

        if case == 3 or case is None:
            ''' Initialization 
                - with logger.   
                - with manual dir name.  
                - with manual file name.               
            '''
            self.create_reff_mydb(self.mainPath, dbName, logLevel=logLevel, dirName=dirName, fileName="USP")

     
class TestMyDB(TestUtils, TestMyDBReff):
    def __init__(self, methodName: str = "runTest") -> None:
        TestUtils.__init__(self)
        TestMyDBReff.__init__(self)

    # Infokes Ones at start executed
    @classmethod
    def setUpClass(cls):
        print_title("setUpClass") 
        # print('\nsetUpClass\n')
        cls.maxIndexICases = 4
        cls.mainPath =  os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))))  

        rootDir =  os.path.join(cls.mainPath, "root")
        if os.path.isdir(rootDir):
            shutil.rmtree(rootDir)

    # Infokes Ones at end executed
    @classmethod
    def tearDownClass(cls):
        print_title('tearDownClass')

    def tearDown(self):
        print(' - tearDown')
        # Detach connection to database to close connection to .ldb file.

        if os.path.isfile(self.dbFile):
            with self.db.engine.connect() as connection:               
                try: 
                    with connection.begin():
                        if exec:
                            connection.detach()
                except Exception as error:
                    raise Exception(f"Removing database file tree failed: {error}")
        try:
            if os.path.isdir(self.dbDir):
                shutil.rmtree(self.dbDir)
        except:
             pass

        # if os.path.isdir(self.dbDir):
        #     shutil.rmtree(self.dbDir)
        # if os.path.isdir(self.BuDir):
        #     shutil.rmtree(self.BuDir)
        pass
    
    def runTest(self):
        print_title('runTest')
        caseList =  range(self.maxIndexICases)
        caseList =  [1]
        _print = True
        for c in caseList:
            print(c)
            # self.init_test(c, _print=_print)
            # self.set_test(c, _print=_print)
            # self.create_test(c, _print=_print)
            # self.remove_test(c, _print=_print)
            # self.backup_test(c, _print=_print)
            self.table_test(c, _print=_print)

    ######################################################################
    ###               Test functions of the object.                    ###
    ######################################################################
    def init_test(self, case:int, _print:bool = False):
        print_title('init_test')

        # Select inization case and create reference
        self.init_cases_mydb(case, _print = _print) 

        # Test general variables
        txt = f"Test database main path."  
        self.equal_test("mainPath", self.mainPath, self.db.mainPath, expl=txt, _print=_print)

        txt = f"Test database name."  
        self.equal_test("dbName", self.dbName, self.db.name, expl=txt, _print=_print)
                  
        txt = f"Test database path: Backup directory."  
        self.logger_test("MyDB", self.logLevel, self.db.logLevel, self.db, _print=_print)

        # Auto initialization with log.
        if case == 0 or case == 1:
            # Test if the set method invoked during initialization.
            self.connect_false_test(self.db, self.db.engine, _print = _print)
            
            # Create a new database to invoke the set method.
            engine = self.db.create(self.dirName, self.fileName, self.fileType)

            # Test return parameter
            self.connect_true_test(self.db, engine, _print = _print)
            # Test property of object created
            self.connect_true_test(self.db, self.db.engine, _print = _print)
            self.tearDown()
        
    def set_test(self, case:int, _print:bool = False):
        print_title('set_test')   
         
        # Select inization case and create reference
        self.init_cases_mydb(case, _print = _print) 
        
        if not self.db.connected:
            self.connect_false_test(self.db, self.db.engine, _print = _print)            

            result  = self.db.create(dirName=self.dbDir, fileName=self.fileName, fileType=self.fileType)
            self.assertTrue(result, "Test if db is created.")

        # Test return parameter
        self.connect_true_test(self.db, self.db.engine, _print = _print)

        # Test path's created 
        txt = f"Test database path: Directory path."  
        self.equal_test("dbDir", self.dbDir, self.db.dbDir, expl=txt, _print=_print)

        txt = f"Test database path: File path."  
        self.equal_test("dbFile", self.dbFile, self.db.dbFile, expl=txt, _print=_print)

        txt = f"Test database path: Main backup directory."  
        self.equal_test("mBuDir", self.mBuDir, self.db.mBuDir, expl=txt, _print=_print)
        
        txt = f"Test database path: Backup directory."  
        self.equal_test("buDir", self.buDir, self.db.buDir, expl=txt, _print=_print)

        self.tearDown()

    def create_test(self, case:int, _print:bool = False):
        print_title('create_test')   

        # Select inization case and create reference
        self.init_cases_mydb(case, _print = _print) 

        result  = self.db.create(dirName=self.dbDir, fileName=self.fileName, fileType=self.fileType)
        self.assertTrue(result, "Test if db is created.")

        txt = f"Test database dir path."  
        self.path_true_test("dbDir", self.dbDir, self.db.dbDir, expl=txt, _print=_print)

        txt = f"Test database file path."  
        self.path_true_test("dbFile", self.dbFile, self.db.dbFile, expl=txt, _print=_print)

        self.tearDown()
    
    def remove_test(self, case:int, _print:bool = False):
        print_title('remove_test')      

        # Select inization case and create reference
        self.init_cases_mydb(case, _print = _print)   

        result  = self.db.create(dirName=self.dbDir, fileName=self.fileName, fileType=self.fileType)
        self.assertTrue(result, "Test if db is created.")

        self.path_true_test("dbDir", self.dbDir, self.db.dbDir, _print=_print)
        self.path_true_test("dbFile", self.dbFile, self.db.dbFile, _print=_print)

        self.db.remove(exec=True) 

        self.path_false_test("dbDir", self.dbDir, self.db.dbDir, _print=_print)
        self.path_false_test("dbFile", self.dbFile, self.db.dbFile, _print=_print)

    def backup_test(self, case:int, _print:bool = False):
        print_title('backup_test')      

        # Select inization case and create reference
        self.init_cases_mydb(case, _print = _print)  
        result  = self.db.create(dirName=self.dbDir, fileName=self.fileName, fileType=self.fileType)
        self.assertTrue(result, "Test if db is created.")

        txt = f"Test database dir path."  
        self.path_true_test("dbDir", self.dbDir, self.db.dbDir, expl=txt, _print=_print)

        txt = f"Test database file path."  
        self.path_true_test("dbFile", self.dbFile, self.db.dbFile, expl=txt, _print=_print)
        
        result = self.db.backup()

        txt = f"Test database path: Main backup directory."  
        self.path_true_test("mBuDir", self.mBuDir, self.db.mBuDir, expl=txt, _print=_print)
        
        txt = f"Test database path: Backup directory."  
        self.path_true_test("buDir", self.buDir, self.db.buDir, expl=txt, _print=_print)

        
        txt = f"Test database path: Dest Backup Directory."  
        self.path_true_test("dDuDir", self.dDuDir, result, expl=txt, _print=_print)
        self.tearDown()

    def table_test(self, case:int, _print:bool = False):
        print_title('table_prop_test')           

        # Select inization case and create reference
        self.init_cases_mydb(case, _print = _print)  
        result  = self.db.create(dirName=self.dbDir, fileName=self.fileName, fileType=self.fileType)
        self.assertTrue(result, "Test if db is created.")
        
        table = self.db.add_table("test", TESTTABLE1, TESTDF1) 
        self.assertTrue("Table" in str(type(table)),            "Test if Table is initized.") 
        self.connect_true_test(table, table.engine, _print=_print)                  
        print(self.db.tablenames)
        print(table.name)
        self.db.remove_table("test")
        self.tearDown()
        
    ######################################################################
    ###               Test properties of the object.                   ###
    ######################################################################

    def property_test(self, _print:bool = False):
        def pro_test(name, para, ref):
            txt = f"Test database property: {name}"  
            self.equal_test("name", self.dbName, self.db.name, expl=txt, _print=_print)

        pro_test("name", self.dbName, self.db.name)
        pro_test("present", True, self.db.present)
        pro_test("mainPath", self.mainPath, self.db.mainPath)
        self.connect_true_test(self.db, self.db.engine)

         

       
    def name_test(self):
        print (f" -> Ref dbName:    ", self.dbName)
        print (f" => dbName:        ", self.db.name)
        self.assertEqual(self.dbName, str(self.db.name),                  "Test Check database name.")  
                    

        
if __name__ == "__main__":
    # unittest.main()
    
    if True:
        suite = unittest.makeSuite(TestMyDB, 'Test MyDB')
        # suite.addTests([TestMMultiplication(), TestAddition(), TestDivition()])
                            
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        # print('Error: ', [x[0] for x in result.errors])
        # print('\nFailures: ', [x[-1] for x in result.failures])
        # print('\nSkipped Tests: ', result.skipped)
        # print('\nNo. of Tests: ', result.testsRun)
        # print('\nWas is a succesful test? ', result.wasSuccessful())
        # unittest.main()    
    if False:
        # Create main path for database
        mainPath =  os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))), "test")
        print(f"mainPath: {mainPath}" )    

        # Create database name and create folder
        x = 0
        while True:
                if x == 0:
                        dbName = "test_database"
                else:
                        dbName = f"test_database_{x}"

                dbdir = os.path.join(mainPath, dbName)
                if not os.path.isdir(dbdir):
                        break
                x+=1

        x=0
        while True:
                dirName = f"database_{x}"
                dirPath = os.path.join(mainPath, dirName)
                if not (os.path.isdir(dirPath)):
                        break
                x += 1

        # print(dirPath)
        import shutil 
        import urllib 
        dbFile = r"C:\Users\vpol\OneDrive (HOME)\OneDrive\Documenten\Programming\Python Scripts\modules\controldb\test\database_0"
        con_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
        con_string += f'DBQ={dbFile};'
        con_string += r'Uid=Admin;'
        con_string += r'Pwd=;'
        con_string += r'ExtendedAnsiSQL=1;'

        # Create Engine
        from sqlalchemy import create_engine, Engine
        # connection_url = URL.create("access+pyodbc", query={"odbc_connect": con_string})
        connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(con_string)}"
        engine = create_engine(connection_url)
        t = Table(engine, "TestMyDB1", mainPath, dbName="Test", logLevel=10)
        exit()
        exit()
        
        
        # db = ControlDB(mainPath, dirName=dirName, logLevel=10)
        # db = ControlDB(mainPath, dbName="Manual dbName", dirName="database", fileName="USP", logLevel=10)
        # Create database in containing directory
        # db.create()
        # columns =  TestMyDB1
        # data =  TESTRECORD1
        # db.append_table()

        # # Connect to database
        # db.connect()
        # engine = db.engine
        




        # t = Table("TestMyDB", engine, "Test", logLevel=10)
        # pass
