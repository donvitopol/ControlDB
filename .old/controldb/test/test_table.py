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
from test_utils import TestUtils
from test_mydb import TestMyDBReff
from test_controldb import TestControlDBReff
from test_data import TESTTABLE1, TESTRECORD1, TESTDF1

     
from utils import print_title
from controldb import ControlDBGet, ControlDB
from table import Table
######################################################################
###               Create references For The Test.                 ###
######################################################################

class TestTableDBReff(TestMyDBReff):
    def __init__(self, methodName: str = "runTest") -> None:
        # super().__init__(methodName)
        TestMyDBReff.__init__(self)

    def create_reff_table(self, dbFile:str, tableName:str, **kwargs):  
        print("\n\n  - create_reff_table") #
           
        print("     => Ref fileName:    ", self.fileName)

        self.db.create(dirName=self.dbDir, fileName=self.fileName, fileType=self.fileType)
        # Contruct Tables
        self.table = Table(self.dbFile, tableName, logLevel=self.logLevel)
        
        self.tableName = tableName
        print("     => Ref tableName:    ", self.tableName)
        self.tableNames = self.table.get.table_names() 
        print("     => Ref tableNames:    ", self.tableNames)

        if tableName == "MolTable":
                self.mainID = "Mol_ID"  
        else:
                self.mainID = "ID"  

        table = self.db.add_table("test", TESTTABLE1, TESTDF1) 
        self.amountRecords = len(list(TESTDF1.index))
        print("     => Ref amountRecords:    ", self.amountRecords)
        self.amountTables = len(self.tableNames)
        print("     => Ref amountTables:    ", self.amountTables)
        # exit()


    def init_cases_table(self, case:int=None, _print = False):
        print("\n\n  - init_cases") #
        # Auto initialization
        
        # Select inization case and create reference
        self.init_cases_mydb(case, _print = _print)
        tableName = "Test"
        print("     => Ref tableName:    ", tableName)
        self.create_reff_table(self.dbFile, tableName)
                 
class TestTable(TestUtils, TestTableDBReff):
    def __init__(self, methodName: str = "runTest") -> None:
        TestUtils.__init__(self)
        TestTableDBReff.__init__(self)

    # Infokes Ones at start executed
    @classmethod
    def setUpClass(cls):
        print('\nsetUpClass\n')
        cls.mainPath =  os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))))  
        cls.maxIndexICases = 4
    # Infokes Ones at end executed
    @classmethod
    def tearDownClass(cls):
        print('\ntearDownClass')

    def tearDown(self):
        print('\ntearDown')
        # if os.path.isdir(self.dbDir):
        #     shutil.rmtree(self.dbDir)
        # if os.path.isdir(self.mBuDir):
        #     shutil.rmtree(self.mBuDir)
        # pass
    
    def runTest(self):
        print_title('runTest')
        caseList =  range(self.maxIndexICases)
        caseList =  [1]
        _print = True
        for c in caseList:
            self.init_test(c, _print=_print)



        # self.test_setup()
        # self.backup_manual_test()
        # self.init_test()
        # self.create_test()
        # self.connect_test()
        # self.backup_test()

        # self.remove_test()
        pass

    ######################################################################
    ###               Test functions of the object.                    ###
    ######################################################################

    def init_test(self, case:int, _print:bool = False):
        print('init_test')    
        self.init_cases_table(case, _print=_print) 


    ######################################################################
    ###               Test properties of the object.                   ###
    ######################################################################


    ######################################################################
    ###               Test small occuring errors  .                    ###
    ######################################################################


 
        

        
if __name__ == "__main__":
    # unittest.main()
    
    if True:
        suite = unittest.makeSuite(TestTable, 'Test Table')
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
        t = Table(engine, "TestTable1", mainPath, dbName="Test", logLevel=10)
        exit()
        exit()
        
        
        # db = ControlDB(mainPath, dirName=dirName, logLevel=10)
        # db = ControlDB(mainPath, dbName="Manual dbName", dirName="database", fileName="USP", logLevel=10)
        # Create database in containing directory
        # db.create()
        # columns =  TESTTABLE1
        # data =  TESTRECORD1
        # db.append_table()

        # # Connect to database
        # db.connect()
        # engine = db.engine
        




        # t = Table("TestTable", engine, "Test", logLevel=10)
        # pass
