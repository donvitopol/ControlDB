
import os, sys
from pprint import pprint

import pandas as pd
from sqlalchemy import Engine



from controldb import ControlDBGet
from controldb import ControlDB
from pretty_logger import PrettyLogger, prettylog
# path =  os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))
# sys.path.append(path)
# print(path)
# exit()
if __name__ == "main":
        from test.test_data import TESTTABLE1, TESTRECORD1
# print(ControlDB("test"))
# print(ControlDB("test").mainPath)
# print(type(PrettyLogger))
# print(type(ControlDB))

# class Table(ControlDB):
#         # Start up
#         def __init__(self): 
#                pass  
# exit()
@prettylog
class Table(ControlDB):
        # Start up
        def __init__(self, dbFile:str, tableName:str, logLevel:int=30):   
        # def __init__(self, dbEngine:Engine, tableName:str, mainPath:str, dbName:str="root",   
        #              dirName=None, fileName=None, buDir=None, fileType:str="mdb", logLevel=30):
                '''
                Input Parameters:
                        - dbEngine(Engine): 
                        - tableName(str): name of the table, 
                        - mainPath(str): Main path of the database 
                        - dbName(str): Name of the database   
                        - dirName(str):
                        - fileName(str):
                        - buDir(str): 
                        - fileType(str): 
                        - logLevel(str):
                '''
                ControlDB.__init__(self, logLevel=logLevel)   

                self.connect(dbFile)
                self.tableName = tableName
                
                if tableName == "MolTable":
                        self.mainID = "Mol_ID"  
                else:
                        self.mainID = "ID"  

            
                # Asign Logger
                self.logger:PrettyLogger
                self.get:ControlDBGet
                # exit()

                # self.columnNames = self.get.column_names(self.tableName)

        # @property
        # def connected(self) -> Engine:
        #         return True 
        
        @property
        def engine(self) -> Engine:
                return super().engine   
        
        @engine.setter
        def engine(self, _engine)->Engine:
                self.connected = True
                super().engine = _engine

        @property
        def name(self) -> str:
                return self.tableName        

        @property
        def columnnames(self) -> list:
                return self.get.table_names()
        
        @property
        def column(self, column:str) -> dict[str:any]:
                print(" Get column")
                return self.get.column(self.tableName, column)
        
        @property
        def columns(self, columns:str) -> dict[str:any]:
                print(" Get columns")
                return self.get.columns(self.tableName, columns)
              
        def append_record(self, record:dict)->None: 
                super().append_record(self.tableName, record)
            
        def remove(self)->None:  
                self.logger.debug('  - Table -> remove ')           
                super().remove_table(self.tableName)

        def get_item(self, id:int, column:str)->any:
                return self.get.cell(self.tableName, column, id, idColumn=self.mainID)
         
        def get_id(self, column:str, item:str)->int:
                return self.get.cell(self.tableName, self.mainID, f"'{item}'", idColumn=column)

        def get_column_data(self, column:str|list)->any:
                return self.get.column(self.tableName, column)
        
        def get_columns(self, column:list)->any:
                return self.get.columns(self.tableName, column)
        
        def update_record(self, id, record):
                return self.update_record(self.tableName, id, record, idColumn=self.mainID)
        
        def append_table(self, record:dict)->None: 
            self.append_table(self.tableName, record)

        def set_record(self, record:dict)->None:
                # Update the record
                empty = False
                log = " -> "
                if not empty:
                    id = record.pop(self.mainID)
                    self.update_record(id, record)
                    log += f"Update record {self.dbName}! => Table: {self.tableName}, {self.mainID}: {id}" 
                else:
                    self.append_table(record)
                    log += f"Append record {self.dbName}! => Table: {self.tableName}, {self.mainID}: {id}" 


                return id
    
        def insert_last_row(self, column:int, value:any)->None:
            self.insert_last_row(self.tableName, column, value, idColumn=self.mainID)

        def delete_record(self, id:int)->None:
            self.delete_row(self.tableName, id, idColumn = self.mainID)

        def clear_record(self, id:int)->None:
            # self.__db.delete_row(self.tableName, id, idColumn = self.mainID)
            pass

        def info(self)->None:
            self.logger.info(f" -> Table Class => DB Name: {self.dbName}, Table Name: {self.tableName}, Main ID Key: {self.mainID}")
            pass


# if __name__ == '__main__': 
    
#         # Create main path for database
#         mainPath =  os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))), "test")
#         print(f"mainPath: {mainPath}" )    

#         # Create database name and create folder
#         x = 0
#         while True:
#                 if x == 0:
#                         dbName = "test_database"
#                 else:
#                         dbName = f"test_database_{x}"

#                 dbdir = os.path.join(mainPath, dbName)
#                 if not os.path.isdir(dbdir):
#                         break
#                 x+=1

#         x=0
#         while True:
#                 dirName = f"database_{x}"
#                 dirPath = os.path.join(mainPath, dirName)
#                 if not (os.path.isdir(dirPath)):
#                         break
#                 x += 1

#         # print(dirPath)
#         import shutil 
#         import urllib 
#         dbFile = r"C:\Users\vpol\OneDrive (HOME)\OneDrive\Documenten\Programming\Python Scripts\modules\controldb\test\database_0"
#         con_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
#         con_string += f'DBQ={dbFile};'
#         con_string += r'Uid=Admin;'
#         con_string += r'Pwd=;'
#         con_string += r'ExtendedAnsiSQL=1;'

#         # Create Engine
#         from sqlalchemy import create_engine, Engine
#         # connection_url = URL.create("access+pyodbc", query={"odbc_connect": con_string})
#         connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(con_string)}"
#         engine = create_engine(connection_url)
#         t = Table(engine, "TestTable1", mainPath, dbName="Test", logLevel=10)
#         exit()
#         exit()
        
        
#         # db = ControlDB(mainPath, dirName=dirName, logLevel=10)
#         # db = ControlDB(mainPath, dbName="Manual dbName", dirName="database", fileName="USP", logLevel=10)
#         # Create database in containing directory
#         # db.create()
#         # columns =  TESTTABLE1
#         # data =  TESTRECORD1
#         # db.append_table()

#         # # Connect to database
#         # db.connect()
#         # engine = db.engine
      




#         # t = Table("TestTable", engine, "Test", logLevel=10)
#         # pass
