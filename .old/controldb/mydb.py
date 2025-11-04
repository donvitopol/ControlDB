

import logging
import shutil 
from datetime import datetime, date
from pprint import pprint
from sqlalchemy import Engine
import pandas as pd
from pandas import DataFrame
import msaccessdb

from pretty_logger import PrettyLogger, prettylog


from controldb import ControlDB, ControlDBGet
from table import Table
# print("MyDB")
# exit()

@prettylog
class MyDB(ControlDB):
        def __init__(self, mainPath:str, dbName:str, buDir=None,  
                logLevel=30, dirName=None, fileName=None,  
                fileType:str="mdb")->bool:
                """This object is created to be inherited to have all control over a MS Access Database.
                
                        Input Parameters:
                                - mainPath(str):        The path to the project folder.
                                - dbName(str):          The name of the database.
                                - logLevel(int):        The log level that will be displayed in terminal
                                - dirName(str):         The name of the directory.
                                - fileName(str):        The name of the database file.
                                - fileType(str):        The type of the database file.
                                - buDir(str):           The path where the backups are stored.

                        Return Parameters:   
                                connected(bool):        Returns if database could connect or not
                """
                # Assign variables
                self.__mainPath:str = mainPath 
                self.__dbName:str   = dbName
                self.buDir:str   = buDir

                self.dbDir:str
                self.dbFile:str
                self.mBuDir:str
                self.__tables:dict[Table] = {}


                # Start stream for logger
                self.logger:PrettyLogger  
                self.logLevel:int=logLevel

                ControlDB.__init__(self, logLevel=logLevel)    
                self.get:ControlDBGet
                self.engine:Engine
                
                if self.__set_file(dirName, fileName, fileType):
                        self.set(dirName=dirName, fileName=fileName, fileType=fileType)

        def __set_file(self, dirName, fileName, fileType):
                # Check if defaut name is used
                if dirName is None:
                        dirName = self.__dbName
                if fileName is None:
                        fileName = "database"

                # Contruct database path's
                self.dbDir:str = os.path.join(self.__mainPath, dirName)
                # print (f" => dbDir: {self.dbDir}")
                self.dbFile:str = os.path.join(self.dbDir, f"{fileName}.{fileType}")
                # print (f" => dbFile: {self.dbFile}")
                return os.path.isfile(self.dbFile)
                
        def __set_backup(self)->bool: 
                self.logger.debug('  - MyDB -> __set_backup')          
                backupName = "BU_"  + f"{self.__dbName}".replace(" ", "_").lower()
                # print (f" => backupName: {backupName}")
                if not self.buDir is None:
                        self.mBuDir:str = self.buDir
                        self.buDir:str = os.path.join(self.buDir, backupName)
                else:
                        self.mBuDir:str = os.path.join(self.__mainPath, "backup")
                        self.buDir:str = os.path.join(self.mBuDir, backupName)
                # print (f" => mBuDir: {self.mBuDir}")
                # print (f" => buDir: {self.buDir}")

        def set(self, dirName=None, fileName=None, fileType:str="mdb")->bool:
                """Check if the database is present, when prevent connection will be created"""    
                self.logger.debug('  - MyDB -> set')   
                self.__set_file(dirName, fileName, fileType)
                if os.path.isfile(self.dbFile):
                        self.__set_backup()
                        self.connect(self.dbFile) 

                        # Contruct Tables
                        self.tableNames = self.get.table_names()                         
                        for n in self.tableNames: 
                                self.__tables[n] = Table(self.dbFile, n, logLevel=self.logLevel)
                        return True
                return False
        
        def create(self, dirName=None, fileName=None, fileType:str="mdb")->Engine:   
                self.logger.debug('  - MyDB -> create')  
                self.__set_file(dirName, fileName, fileType) 
                # Create a directory for the new database.
                if not os.path.isfile(self.dbFile):
                        # Create a directory for the new database.
                        if not os.path.isdir(self.dbDir):
                                os.mkdir(self.dbDir)

                        super().create(self.dbFile, fileType=fileType)
                        self.set(dirName=dirName, fileName=fileName, fileType=fileType)
                        return super().engine
                else:
                        # File is already present so no database is created
                        return None
                             
        def remove(self, exec:bool=False, level:str="dir")->None:
                ''' Delete database by removing.
                input parameters:
                        - exec(boolean): Check whether to delete the database.
                        - level(str): Level of deleting: ['dir', 'file']       
                
                '''
                self.logger.debug('  - MyDB -> remove') 

                if exec and level == 'file':
                        if os.path.isfile(self.dbFile):
                                try:
                                        super().remove(self.dbFile)
                                        # self.engine = None
                                except:
                                        raise PermissionError("Could not remove file: Close file!")
                                finally:
                                        self.logger.info("Database file is successfully removed.")
                        else:
                                raise FileNotFoundError("Select file of database!")
                        pass
                elif exec and level == 'dir':
                        if os.path.isdir(self.dbDir):
                                try:
                                        super().remove(self.dbFile, exec=exec)
                                        shutil.rmtree(self.dbDir)
                                        # self.engine = None
                                except:
                                        raise PermissionError("Could not remove directory: Close folders and file's in directory tree!")
                                finally:
                                        self.logger.info(" -> Database directory is successfully removed.")
                        else:
                                raise NotADirectoryError("Select directory of database!")
                else:
                        raise ValueError("No level specified te delete database, provided level!")
                pass

        def backup(self, remark=None)->str:
                self.logger.debug('  - ControlDB -> backup') 
                # Create a directory to backup database.
                self.logger.debug(f" => mBuDir: {self.mBuDir}")
                if not os.path.isdir(self.mBuDir):
                        os.mkdir(self.mBuDir)
                self.logger.debug(f" => buDir: {self.buDir}")
                if not os.path.isdir(self.buDir):
                        os.mkdir(self.buDir)
                        
                t:datetime = datetime.now() 
                x = 0 
                while True:
                        if x == 0:
                                dirName = f"{date(t.year, t.month, t.day)} - {self.__dbName}"
                        else:
                                dirName = f"{date(t.year, t.month, t.day)} ({x}) - {self.__dbName}"
                        
                        if not remark == None:
                                dirName += " - " + remark 

                        destDir =  os.path.join(self.buDir, dirName)
                        if not os.path.isdir(destDir):  
                                break
                        x += 1   

                self.detach()
                self.logger.debug(f" => destDir: {destDir}")
                shutil.copytree(self.dbDir, destDir)
                self.logger.info(f' -> BackUp Created')    
                self.connect(self.dbFile)                
                return destDir

        def add_table(self, name:str, header:dict, data:DataFrame=None)->Table:
        # def add_table(self, val:tuple[str, dict, DataFrame])->Table:
                self.logger.debug('  - MyDB -> add_table ') 

                table:Table = Table(self.dbFile, name, logLevel=self.logLevel)                
                
                if self.append_table(name, header):
                        self.logger.info(' -> New table created.')

                if not data is None:
                        self.logger.info(' -> Import data to the new table.')                        
                        for i in data.index:
                                table.append_record(data.loc[i].to_dict())                                
                self.table = name, table
                return table
        
        def remove_table(self, name:str)->Table:
                self.logger.debug('  - MyDB -> remove_table ')                 
                table:Table = self.__tables.pop(name)
                print(table.name)
                print(self.__tables)
                table.remove()
                print(self.__tables)

        @property
        def present(self)->bool:
                if os.path.isfile(self.dbFile):
                        return True
                else:
                        return False
                
        @property
        def mainPath(self)->str:
                return self.__mainPath
        
        @property
        def name(self) -> str:
                return self.__dbName
        
        @property
        def engine(self) -> str:
                return super().engine
        
        @engine.setter
        def engine(self, _engine) -> None:
                super().engine = _engine

        @property
        def tablenames(self) -> list:
                return self.get.table_names()

        @property
        def table(self, name:str) -> Table:
                print(" Get table")
                return self.__tables[name]

        @table.setter
        # def table(self, name:str, header:dict, data:DataFrame=None)->Table:
        def table(self, val:tuple[str, Table])->None:
                # self.logger.debug('  - MyDB -> table setter ') 
                # self.logger.debug(f" => val: {val}, inputN: {len(val)}")
                try:
                        name, table = val
                except ValueError:
                        raise ValueError("Pass an iterable with two items")
                else:   
                        """ This will run only if no exception was raised """
                self.__tables[name] = table
                
        def info(self)->None:
                self.logger.info(f" -> info database => Database: {self.name}, Tables: {self.tablenames}")
                pass  
         
import os
if __name__ == '__main__': 
    from test.test_data import TESTTABLE1, TESTRECORD1, TESTDF1
    # Create main path for database
    mainPath =  os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))), "test")
    print(mainPath)    

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
        dirPath = os.path.join(mainPath, dbName, dirName)
        if not (os.path.isdir(dirPath)):
            break
        x += 1
    print(dirPath)  
    
    
    # db = ControlDB(mainPath, dirName=dirName, logLevel=10)
    db = MyDB(mainPath, dbName, dirName=dirName, fileName="USP", logLevel=10)
    
    db.create()

    db.add_table("test", TESTTABLE1, TESTDF1)
    exit()




    
    
    exit()