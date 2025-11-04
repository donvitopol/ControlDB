

import logging
from pprint import pprint
from sqlalchemy import Engine

from pretty_logger import PrettyLogger

from .controldb import ControlDB, ControlDBGet
from .table import Table
from ..helper import *

@prettylog
class DataBase(ControlDB):
    def __init__(self, dbName:str, dbPathDec, logLevel=30):
                self.dbName = dbName
                self.tableNames:list
                
                # Asign Logger
                self.logger:PrettyLogger

                # Define DataBase Path
                if dbName == "root":
                        self.dbPath = dbPathDec()
                else:
                        self.dbPath = dbPathDec(dbName)   

                # Connect Main DataBase
                self.get:ControlDBGet
                ControlDB.__init__(self, logLevel=logLevel)   
                engine:Engine = self.connect(self.dbPath, dbName=dbName) 
                
                # Contruct Tables
                self.tableNames = self.get.table_names() 
                self.__tables:dict[Table] = {}
                for n in self.tableNames: 
                        self.__tables[n] = Table(self.dbName, engine, n, logLevel=logLevel)
                        if logLevel <= 20:
                                self.__tables[n].info()


    def get_name(self)->str:
            return self.dbName
    
    def get_keys(self)->str:
            return self.tableNames
    
    def get_table(self, name:str)->Table:
            return self.__tables[name]
    
    def get_columns(self, column:list)->any:
            return self.get.columns(self.tableNames, column)
    
    def get_column_names(self, column:list)->any:
            return self.get.column_names(self.tableNames)
    
    def info(self)->None:
        self.logger.info(f" -> info database => Database: {self.dbName}, Tables: {self.tableNames}")
        pass  
    