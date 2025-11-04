
import os
from pprint import pprint

import pandas as pd
from sqlalchemy import Engine
from pretty_logger import PrettyLogger

from .controldb import ControlDB
from .controldb import ControlDBGet
from ..helper import *


@prettylog
class Table(ControlDB): 
        # Start up
        def __init__(self, dbName:str, dbEngine:Engine, tableName:str, logLevel=30):
                self.dbName = dbName
                self.tableName = tableName
            
                # Asign Logger
                self.logger:PrettyLogger

                ControlDB.__init__(self, logLevel=logLevel)   
                self.get:ControlDBGet
                self.set_engine(dbEngine) 

                self.columnNames = self.get.column_names(self.tableName)

                if tableName == "MolTable":
                        self.mainID = "Mol_ID"  
                else:
                        self.mainID = "ID"  

        def get_column_names(self)->str:
                return self.columnNames

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
