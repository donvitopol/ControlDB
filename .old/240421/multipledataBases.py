


import logging

from pretty_logger import PrettyLogger

from .controldb import ControlDB
from .mydb import DataBase
from ..helper import *

    
@prettylog
class MultipleDataBases():
    def __init__(self, mdbPath, sdbPath, tableKey:str, columnKey:str, logLevel=30):
        self.mdbPath = mdbPath
        self.sdbPath = sdbPath
        self.logLevel = logLevel

        # Asign Logger
        self.logger:PrettyLogger

        # Internalize Root DataBases
        self.__dbList:dict[DataBase] = {}   
        t = DataBase('root', self.mdbPath, logLevel=logLevel)  
        self.__dbList["root"] = t

        # Request Table Names
        self.sdbNames:list = t.get.column(tableKey, columnKey)

        # Internalize Sub DataBases
        for n in self.sdbNames: 
            self.__dbList[n] = DataBase(n, self.sdbPath, logLevel=logLevel)          


    def get_db(self, dbName:str):
        return self.__dbList[dbName]
    
    def get_table(self, dbName:str, tableName:str):
        return self.__dbList[dbName].get_table(tableName)
    
    def add_db(self, name):
        self.__dbList[name] = DataBase(name, self.sdbPath, self.logLevel)
        return self.__dbList[name]
    
    def info(self):        
        self.logger.info(f" -> Names Of Sub Databases: {self.sdbNames}")
    
        