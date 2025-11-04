#!/usr/bin/python 3.11.5
'''
References:
 -  https://www.w3schools.com/python/python_mysql_insert.asp
 -  https://www.cas.org/services/commonchemistry-api
    API to request density information

Help:
 - Data type mappings between Python and SQL Server 
    https://learn.microsoft.com/en-us/sql/machine-learning/python/python-libraries-and-data-types?view=sql-server-ver16

Modules to install 
 - pyodbc
 - pandas
 - pyodb
 - sqlalchemy
 - sqlalchemy-access
 - msaccessdb
 - rdkit
 - bs4
 - requests
 - openpyxl
 - environs

 pip install pyodbc pandas pyodb sqlalchemy sqlalchemy-access msaccessdb rdkit bs4 requests openpyxl environs
'''

import logging
import os
from datetime import datetime, date
from pprint import pprint

import pyodbc
import urllib
import pandas as pd
from sqlalchemy import create_engine, Engine
from sqlalchemy.engine import URL
import msaccessdb

from pretty_logger import PrettyLogger
from ..helper import *

@prettylog
class ControlDBGet():
    def __init__(self, engine:Engine, logLevel=30)->None:
        self.__engine:Engine = engine 
        self.logger:PrettyLogger


    def cell(self, table:str, column:str, id:int, idColumn:str="ID")->any:
        self.logger.debug(f'  - ControlDBGet -> cell')   
        sql = f'SELECT {column} from {table} WHERE {idColumn} = {id};'
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()   
                    cursor.execute(sql)
                    # print(cursor.fetchone())
                    value = cursor.fetchone()[0]
                    # self.logger.info(f' => Requested value: [{value}]')
                    connection.close()  
                    return value 
            except:
                self.logger.warning(f" =! Get cell: FAILED")
                self.logger.debug(f' => Table: {table}, Column: {column}, id: {id}, idColumn: {idColumn}')
                pass
            pass
        pass

    def table(self, tableName:str)->None:
        ''' Show table of Users.
        
        '''
        self.logger.debug(f'  - ControlDBGet -> table')   
        sql = f'SELECT * FROM {tableName}'
        with self.__engine.connect() as connection: 
            try: 
                with connection.begin():
                    df = pd.read_sql_query (sql, connection.connection)
                    connection.close()  
                    return df
            except:
                self.logger.warning(f" =! Get Table: FAILED")
                pass
            pass
        pass

    def column(self, tableName:str, columnName:str)->list:
        ''' Show table of Users.
        
        '''
        self.logger.debug(f'  - ControlDBGet -> column')   
        sql = f'SELECT {columnName} FROM {tableName}'
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()  
                    # Set cursor of the database
                    cursor.execute(sql)
                    data = [x[0] for x in cursor.fetchall()]
                    connection.close()  
                    
                    return data
            except:
                self.logger.warning(f" =! Get column: FAILED")
                pass
            pass
        pass

    def columns(self, tableName:str, columnNames:list)->list[tuple]:
        ''' Show table of Users.
        
        '''
        self.logger.debug(f'  - ControlDBGet -> columns')   
        h = ", ".join(columnNames)
        sql = f'SELECT {h} FROM {tableName}'
        # sql = f'SELECT ID,CodeLogic FROM CodeLogicTable'
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()  
                    # Set cursor of the database
                    cursor.execute(sql)
                    # print(cursor)
                    # print(cursor.fetchall())
                    data = cursor.fetchall()
                    connection.close()  
                    # print(cursor.fetchone())
                    return data
            except:
                self.logger.warning(f" =! Get column: FAILED")
                self.logger.warning(f" => Sql = {sql}")
                pass
            pass
        pass
    
        # with self.__engine.connect() as connection:         
        #     # try: 
        #         with connection.begin():
        #             cursor = connection.connection.cursor()  
        #             # Set cursor of the database
        #             cursor.execute(sql)
        #             # print(cursor)
        #             # print(cursor.fetchall())
        #             data = cursor.fetchall()
        #             connection.close()  
        #             # print(cursor.fetchone())
        #             return data
        # pass

    def table_names(self)->list:
        ''' Show table of Database.
        
        '''
        with self.__engine.connect() as connection:      
            try: 
                with connection.begin():
                    # Set cursor of the database
                    cursor = connection.connection.cursor()  
                    tableNames = [name.table_name for name in cursor.tables() if not "MSys" in name.table_name]
                    connection.close() 

                    return tableNames
            except:
                self.logger.warning(f" =! Get table names: FAILED")
                # raise Exception('No connection!')
            pass
             
    def column_names(self, tableName:str)->None:
        ''' Show table of Users.
        
        '''
        self.logger.debug(f'  - ControlDBGet -> column_names')    

        sql = f'SELECT * FROM {tableName}'
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    # Set cursor of the database
                    cursor = connection.connection.cursor()  
                    cursor.execute(sql)
                    columnNames = [c[0] for c in cursor.description]
                    connection.close() 

                    return columnNames
            except:
                self.logger.warning(f" =! Get column names: FAILED")
                pass
            pass
        pass
    
    def next_id(self, tableName:str, name:str="ID")->int:
        self.logger.debug(f'  - ControlDBGet -> next_id')      
        sql = f"SELECT MAX({name}) FROM {tableName}"  
        with self.__engine.connect() as connection:                   
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()   
                    cursor.execute(sql)  
                    lastId = cursor.fetchone()[0]

                    if lastId is None:
                        lastId = 1
                    else:
                        lastId += 1
                    connection.close()  
                    self.logger.debug(f' => Next ID for new record: {lastId}')
                    return lastId
            except:
                self.logger.warning(f" =! New ID: Not found")
                self.logger.info(f" -> Table name: {tableName}")
                return None
            pass
        pass

class ControlDB():
    def __init__(self, logLevel=30)->None:
        # Start stream for logger
        self.logLevel:int=logLevel 
        self.logger:PrettyLogger

        self.get:ControlDBGet
        

    

    def connect(self, dbPath:str, dbName=None)->Engine:
        ''' Connect to the database and setting cursor to control the database.
        
        '''
        msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        if not "Microsoft Access Driver (*.mdb, *.accdb)" in msa_drivers:
            if hasattr(self, 'logger'):
                self.logger.critical(f' =! Wrong engine installed of MS-ACCESS Driver install 64 bit Office')
                self.logger.info(f' => MS-ACCESS Drivers: {msa_drivers}') 
            raise ConnectionRefusedError("No Connection!")
        if hasattr(self, 'logger'):
            self.logger.debug(f' => {dbName} DB File: {dbPath}') 
        
        try:
            con_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
            con_string += f'DBQ={dbPath};'
            con_string += r'Uid=Admin;'
            con_string += r'Pwd=;'
            con_string += r'ExtendedAnsiSQL=1;'
            # Create Connection
            # self.logger.debug(f' => Connection String: {con_string}') 
            # connection = pyodbc.connect(con_string)
            # self.conn = pyodbc.connect(con_string, autocommit = True)
            # exit()

            # Create Engine
            # Ref: https://stackoverflow.com/questions/67749611/exporting-pandas-dataframe-into-a-access-table-using-to-sql-generate-error
            # connection_url = URL.create("access+pyodbc", query={"odbc_connect": con_string})
            connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(con_string)}"
            
            engine = create_engine(connection_url)
            self.__engine = engine 
            txt = " -- Connected To Database "
            if dbName:
                txt += f"=> {dbName}" 

            if hasattr(self, 'logger'):
                self.logger.debug(f' => Connection url: {connection_url}') 
                self.logger.warning(txt)   

        except pyodbc.Error as e:
            self.logger.critical(" -! Error in Connection")
            print("Error in Connection", e)

        self.get = ControlDBGet(engine, logLevel=self.logLevel)      
        return engine
        # exit()
        # create engine 
        # https://medium.com/@chanon.krittapholchai/legacy-system-part-1-microsoft-access-database-with-python-d40d336da338

    def set_engine(self, engine)->None:
        self.__engine = engine
        self.get = ControlDBGet(engine)   
    
    def get_engine(self)->Engine:
        return self.__engine
         
    def create_db(self, folderPath:str, fileName:str)->None:
        self.logger.debug(f'  - ControlDB -> create_db')      
        self.dbPath = f'{folderPath}\{fileName}.accdb'
        self.logger.debug(f' => file path: {self.dbPath}')   
        msaccessdb.create(self.dbPath)
        self.logger.info(f' -> File created')
        pass

    def create_table(self, tableName:str, data:dict)->None: 
        heaterStr = ", ".join([f"{k} {i}" for k, i in data.items()])
        # self.logger.debug(f' => Heater string: {heaterStr}')  
        sql = f"CREATE TABLE {tableName} ({heaterStr})"
        # Create new empty table
        with self.__engine.connect() as connection:                   
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()    
                    cursor.execute(sql)                    
                    connection.commit()  
                    connection.close()  
                    self.logger.info(f' -> Created table {tableName}: Done')  
            except:
                self.logger.warning(f" =! Created table {tableName}: Failed")
                self.logger.debug(f' => Query: {sql}') 
                pass
            pass
        pass

    def append_table(self, tableName:str, data:dict)->None:   
        self.logger.debug(f'  - ControlDB -> append_table')   

        # Convert data to string
        heaterStr = ", ".join(data.keys())     
        # pprint(data) 
        # pprint(data.values())
        for index, value in enumerate(data.values()):
            if index == 0:
                if type(value) == int:
                    valueStr = f"{value}"
                else:
                    valueStr = f"'{value}'"
            else:
                if type(value) == int:
                    valueStr += f", {value}"
                else:
                    valueStr += f", '{value}'"

        # Construct the query string
        sql = f"INSERT INTO {tableName} ({heaterStr}) VALUES ({valueStr})"

        # Insert into the database by querying the data
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()   
                    cursor.execute(sql)
                    connection.commit()
                    connection.close()  
                    self.logger.info(f' -> Append table {tableName}: Done')  
                    pass
                pass
            except:
                self.logger.warning(f" =! Append table {tableName}: Failed ")
                self.logger.warning(f' => Heater string: {heaterStr}')    
                self.logger.warning(f' => Value string: {valueStr}')     
                self.logger.warning(f' => SQL Query: {sql}')  

                pass
            pass
        pass

    def insert_last_row(self, table:str, column:str, value:any, idColumn ="ID")->None:
        self.logger.debug(f'  - ShowData -> insert_last_row')    
        newId = self.get.next_id(table)
        # Insert into the database by querying the data
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()   
                    cursor.execute(f'SELECT {column} from {table};')
                    rowList = [x[0] for x in cursor.fetchall()]
                    newRows = sum(x is not None for x in rowList)+1
                    self.logger.debug(f' => newRows: {newRows}, newId: {newId}') 

                    if (newRows) < newId: 
                        sql = f"UPDATE {table} SET {column} = {value} WHERE {idColumn} = {newRows}"
                        cursor.execute(sql)
                        self.logger.info(f' -> Insert new record: Updated')
                    else:
                        sql = f"INSERT INTO {table} (ID, {column}) VALUES ('{newId}', '{value}')"
                        cursor.execute(sql)
                        self.logger.info(f' -> Insert new record: Done')

                    connection.commit()
                    connection.close()  
                    pass
                pass
            except:
                self.logger.warning(f" =! Insert new record: FAILED")
                self.logger.warning(f' => table: {table}, column: {column}, value: {value}') 
                pass
            pass
        pass

    def drop_table(self, tableName:str)->None:
        ''' Show table of Users.
        
        '''
        sql = f'DROP TABLE {tableName}'
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()  
                    # Set cursor of the database
                    cursor.execute(sql)
                    connection.commit()
                    connection.close()  
                    self.logger.info(f" =! Drop table: {tableName}")
            except:
                self.logger.warning(f" =! Drop table: FAILED")
                self.logger.info(f" -> Won't drop table if file is opened")
                pass
            pass
        pass

    def delete_row(self, tableName:str, ID:int, idColumn ="ID")->None:
        ''' Delete row from table.
        
        '''
        sql = f'DELETE FROM {tableName} WHERE {idColumn} = {ID}'
        
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()  
                    # Set cursor of the database
                    cursor.execute(sql)
                    connection.commit()
                    connection.close()  
                    self.logger.info(f" =! DELETE Row: {ID} in Table: {tableName}")
            except:
                self.logger.warning(f" =! DELETE Row: FAILED")
                self.logger.info(f" -> Won't drop table if file is opened")
                pass
            pass
        pass

    def update_record(self, tableName:str, row:int, data:dict, idColumn ="ID"):

        for index, (key, value) in enumerate(data.items()):
            if index == 0:
                if type(value) == int:
                    updateStr = f"{key} = {value}"
                else:
                    updateStr = f"{key} = '{value}'"
            else:
                if type(value) == int:
                    updateStr += f", {key} = {value}"
                else:
                    updateStr += f", {key} = '{value}'"

        sql = f'UPDATE {tableName} '
        sql += f'SET {updateStr} '
        sql += f'WHERE ({idColumn} = {row})'
        # sql = 'UPDATE ItemTable SET Location_ID = 1 WHERE (ID = 3064)'
        self.logger.debug(f' => SQL Query: {sql}')  
        # exit()
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()  
                    # Set cursor of the database
                    cursor.execute(sql)
                    connection.commit()
                    connection.close()  
                    self.logger.info(f" -> Update table: {tableName}")
            except:
                self.logger.warning(f" =! Update table: FAILED")
                self.logger.warning(f' => SQL Query: {sql}')  
                pass
            # pass
        pass

    def in_column(self, tableName, columnName, item)->bool:

        items = self.get.column(tableName, columnName)
        if type(items)==list:
            if item in items:
                self.logger.debug(f' -> In Column: True')
                return True
            else:
                self.logger.debug(f' -> In Column: False')
                return False
        else:
            if item == items:
                self.logger.debug(f' -> In Column: True')
                return True
            else:
                self.logger.debug(f' -> In Column: False')
                return False
        pass
    
    def in_multi_column(self, tableName:str, columnName:str, item:any, condColumn:dict[str, any], idColumn ="ID")->bool:
        
        # items = self.get.column(tableName, columnName)

        # lst = zip (self.get.column(tableName, idColumn), self.get.column(tableName, columnName))
        
        # print(list(lst))
        # exit
        
        # if item in items:
        #     self.logger.info(f' -> In Column: True')
        #     return True
        # else:
        #     self.logger.info(f' -> In Column: False')
        #     return False
        pass
  

            

    

if __name__ == '__main__': 
    exit() 