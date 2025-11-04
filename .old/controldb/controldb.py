#!/usr/bin/python 3.11.5
'''
References:
 - Connection engine
    - https://medium.com/@chanon.krittapholchai/legacy-system-part-1-microsoft-access-database-with-python-d40d336da338
    - https://stackoverflow.com/questions/67749611/exporting-pandas-dataframe-into-a-access-table-using-to-sql-generate-error
    
 
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
import os
import shutil 
from datetime import datetime, date
from time import sleep
import msaccessdb
import pyodbc
from sqlalchemy import create_engine, Engine
from sqlalchemy.engine import URL
import urllib
import pandas as pd
from contextlib import closing

from pretty_logger import PrettyLogger, prettylog

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

                    self.logger.info(f" -> Get column names: DONE")
                    self.logger.debug(f" => Column names: {columnNames}")
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


# @prettylog
class ControlDB():
    def __init__(self, logLevel=30)->None:
        """This object is created to be inherited to have all control over a MS Access Database.
        input parameters:
            - dbFile(str):          The name of the database.
            - logLevel(int):        The log level that will be displayed in terminal

        return None    
            """
        # print(self.__dict__)
        
        self.__file:str 
        self.ldbFile:str 

        self.__engine:Engine = None
        self.connected:bool = False

        self.get:ControlDBGet   
        # Start stream for logger
        self.logger:PrettyLogger
        self.logLevel:int = logLevel 
    
    @property
    def engine(self)->Engine:
        return self.__engine 
    
    @engine.setter
    def engine(self, _engine)->Engine:
        self.connected = True
        self.__engine = _engine
        self.get = ControlDBGet(_engine, logLevel=self.logLevel)  


    @property
    def present(self)->bool:
        if os.path.isfile(self.dbFile):
            return True
        else:
            return False
        
        # @property
        # def dbDir(self)->str:
        #     self.dbDir =  
        
        # @property
        # def dbFile(self)->str:
        #     return 
           
    def create(self, dbFile, fileType:str="mdb")->None:
        # self.__fileType = fileType        
        if hasattr(self, 'logger'):
            self.logger.debug('  - ControlDB -> create_db')   
            self.logger.debug(f' -> dbFile: {dbFile}')
            self.logger.debug(f' -> dbFile present: {os.path.isfile(dbFile)}')

        # Create a new database.
        if not os.path.isfile(dbFile):

            msaccessdb.create(dbFile)
        else:
            raise FileExistsError(f"Give other name for database {dbFile} is already present!")
         
        if hasattr(self, 'logger'):
            self.logger.info(f' -> New Database created, with path: {dbFile}')
        pass

    def connect(self, file:str)->Engine:
        ''' Connect to the database and setting cursor to control the database.
        
        '''
        self.__file = file
        # print (f" => __file: {self.__file}")
        self.ldbFile:str = file.replace(file.split(".")[-1], "ldb")
        # print (f" => ldbFile: {self.ldbFile}")

        if hasattr(self, 'logger'):
            self.logger.debug('  - ControlDB -> connect') 
            self.logger.debug(f' -> Create connection with database.') 

        # Check if computer got requermenteds to connect to database
        msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        if not "Microsoft Access Driver (*.mdb, *.accdb)" in msa_drivers:
            if hasattr(self, 'logger'):
                self.logger.critical(f' =! Wrong engine installed of MS-ACCESS Driver install 64 bit Office')
                self.logger.info(f' => MS-ACCESS Drivers: {msa_drivers}') 
            raise ConnectionRefusedError("No Connection!")        
        
        try:
            con_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
            con_string += f'DBQ={self.__file};'
            con_string += r'Uid=Admin;'
            con_string += r'Pwd=;'
            con_string += r'ExtendedAnsiSQL=1;'
            # Create Connection
            # self.logger.debug(f' => Connection String: {con_string}') 
            # connection = pyodbc.connect(con_string)
            # self.conn = pyodbc.connect(con_string, autocommit = True)
            # exit()

            # Create Engine
            # connection_url = URL.create("access+pyodbc", query={"odbc_connect": con_string})
            connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(con_string)}"
            
            self.__engine = create_engine(connection_url)

        except pyodbc.Error as e:
            if hasattr(self, 'logger'):
                self.logger.critical(f" -! Error in Connection: {e}")

        finally:
            self.connected = True
            if hasattr(self, 'logger'):
                self.logger.debug(f' => Connection url: {connection_url}') 
                self.logger.warning(f" -- Connected To Database.")   
                

        self.get = ControlDBGet(self.__engine, logLevel=self.logLevel)      
        return self.__engine
     
    def detach(self)->None:

        self.logger.debug('  - ControlDB -> detach') 
        # Detach connection to database to close connection to .ldb file.
        with self.__engine.connect() as connection:               
            try: 
                with connection.begin():
                    if exec:
                        connection.detach()
            except Exception as error:
                self.logger.warning(f" =! Closing file: Failed")
                raise Exception(f"Removing database file tree failed: {error}")
            finally:
                result = True  
                self.logger.info(f" -> Connection with engine is disconnected.")
                self.logger.debug(f" -> Connetion Closed: {connection.closed} => ldbFile: {os.path.isfile(self.ldbFile)}")
            
            # self.logger.debug(f" -> ldbFile exists: {os.path.exists(self.ldbFile)} => path: {self.ldbFile}")

    def remove(self, file, exec:bool=False)->None:
        ''' Delete database by removing.

        input parameters:
            - exec(boolean): Check whether to delete the database.
            - level(str): Level of deleting: ['dir', 'file']       
        
        '''
        self.detach()
        if exec and os.path.isfile(file):
            try:
                os.remove(file)
            except:
                raise PermissionError("Could not remove file: Close file!")
            finally:
                self.logger.info("Database file is successfully removed.")
                

        
    def append_table(self, tableName:str, header:dict)->None: 
        self.logger.debug('  - ControlDB -> append_table') 
        heaterStr = ", ".join([f"{k} {i}" for k, i in header.items()])
        # self.logger.debug(f' => Heater string: {heaterStr}')  
        sql = f"CREATE TABLE {tableName} ({heaterStr})"
        # Create new empty table
        with self.__engine.connect() as connection:                   
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()    
                    cursor.execute(sql) 
                    # connection.commit()  
                    # cursor.close()    
                    self.logger.info(f' -> Create table {tableName}: Done')  
            except:
                self.logger.warning(f" =! Create table {tableName}: Failed")
                self.logger.debug(f' => Query: {sql}') 
                result = False 
            finally:
                # connection.detach()
                result = True                     
                self.logger.debug(f" -> ldbFile: {os.path.exists(self.ldbFile)} => path: {self.ldbFile}")
                self.logger.debug(f" -> Connetion Closed: {connection.closed} => path: {self.ldbFile}")
            
        return result

    def remove_table(self, tableName:str)->None:
        ''' Show table of Users.
        
        '''
        # self.detach()
        sql = f'DROP TABLE {tableName}'
        with self.__engine.connect() as connection:         
            try: 
                with connection.begin():
                    cursor = connection.connection.cursor()  
                    # Set cursor of the database
                    cursor.execute(sql)
                    self.logger.warning(f" -> Removed table: {tableName}: DONE")
            except:
                self.logger.warning(f" =! Removed table: {tableName}: FAILED")
                self.logger.info(f" -> Won't drop table if file is opened")
                pass
            pass
        pass

    def append_record(self, tableName:str, data:dict)->None:   
        self.logger.debug(f'  - ControlDB -> append_record')   

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
  
    def info(self):
        self.logger.debug('  - ControlDB -> info')  
        self.logger.info(f' -> Database: {self.dbName}')  
        self.logger.info(f' => Path directry:   {self.dbDir}') 
        self.logger.info(f' => Path file:       {self.dbFile}')  
        self.logger.info(f' => Connected:       {self.connected}')  

            

'''    
import os
if __name__ == '__main__': 


    
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
    db = ControlDB(mainPath, dbName=dbName, dirName=dirName, fileName="USP", logLevel=10)
    exit()
    
    
    # Create database in containing directory
    db.create()

    # Connect to database
    db.connect()
    columns =  {'FullName': 'CHAR',
                'UserName': 'CHAR',
                'Email': 'CHAR',
                'EmployeeNumber': 'INT',
                'Screens': 'INT',
                'Enzymes': 'INT',
                'Hits': 'INT',
                'Included': 'DATETIME'}
    db.append_table("Test", columns)
    tableNames = db.get.table_names()
    print(tableNames)

    # Delete database
    # db.remove(exec=True)
    exit()

    
    db.info()
    # # Connect to database
    # db.connect()

    db.info()

    print(dbdir)
    exit()



        
    exit()


    exit()
    dbfile =  os.path.join(dbPath, "test_database")

    '''