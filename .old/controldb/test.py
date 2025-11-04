import os
import sys
import logging
from datetime import datetime, date
from pprint import pprint

from controldb import ControlDBGet, ControlDB
from table import Table
from mydb import MyDB
# from multipledataBases import MultipleDataBases

from sqlalchemy import Engine, Table, Column


mainPath = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))
# Main database path decorator
def main_path_dec(path):
        path = path
        def wrapper()->str:
                return os.path.join(path, "database", "CI_Screening_Library.mdb")
        return wrapper

# Sub database path decorator
def sub_path_dec(path):
        path = path        
        def wrapper(key)->str:
                fileName = f"CI_Screening_Library_{key}.mdb"
                return os.path.join(path, "libraries", key, "database", fileName)
                
        return wrapper

if (__name__ == '__main__'):

        mainPath = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))
        print(mainPath)
        dbFile = os.path.join(mainPath, "Crypto", f"CryptoSetup.mdb")
        print(dbFile)
        db = MyDB(mainPath, "CryptoSetup", dirName="Crypto",logLevel=10)
        # db.create(dirName="Crypto", fileName="CryptoSetup")
        db.set(dirName="Crypto", fileName="CryptoSetup")

        engine:Engine = db.engine


        from sqlalchemy import Table, MetaData, Column, Integer
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.dialects.mssql import *

        # Base = declarative_base()
        # t = Table('t', MetaData(),
        #         Column('id', Integer, primary_key=True),
        #         Column('x', Integer))
        # m.create_all(engine)
        m = MetaData()
        t = Table('test2', m, Column('id', Integer, primary_key=True),
                        Column('x', Integer))
        
        m.create_all(engine)
        with engine.begin() as conn:
                conn.execute(t.insert(), [{'id': 1, 'x':1}, {'id':2, 'x':2}, {'id':3, 'x':3}])
                conn.close()
        # with engine.begin() as conn:
        #         conn.execute()
        exit()



        with engine as connection:         
                with connection.begin():
                    connection:Engine
                    cursor = connection.connection.cursor()   
                    cursor.execute
                    exit()



#     mP = main_path_dec(mainPath)
#     sP = sub_path_dec(mainPath)
#     # Test Table Class
#     testList = []
#     testList.append('')                             # 0
#     testList.append('ControlDB')                    # 1
#     testList.append('Table')                        # 2
#     testList.append('DataBase')                     # 3
#     testList.append('MultipleDataBases')            # 4
#     run = testList[2]

#     # Test ControlDB Class
#     if run == "Table":
#         print(" -> Test main table")
#         db = ControlDB()
#         engine = db.connect(mP())

#         p:Table = Table("main", engine, "MolTable")
#         print(p.get_column_names())

#         p:Table = Table("main", engine, "LibraryTable")
#         print(p.get_column_names())
#         print(p.get_item(2, "LibraryCode"))
#         print(p.get_id("LibraryCode", "SFF"))
#         print(p.get_column_data("LibraryCode"))
#         p.info()
#         exit()
#         # t:Table = p.main_db

#     # Test DataBase Class
#     if run == "DataBase":
#         path = main_path_dec(mainPath)
#         p:DataBase = DataBase("root", path, logLevel=logging.DEBUG)
#         p.info()

#         path = sub_path_dec(mainPath)
#         p:DataBase = DataBase("NCN", path, logLevel=logging.DEBUG)
#         p.info()
#         exit()

#     # Test MultipleDataBases Class
#     if run == "MultipleDataBases":
#         p = MultipleDataBases(mP,sP, "LibraryTable", "LibraryCode", logLevel=logging.INFO)
#         t = p.get_table("root", "LibraryTable")
#         t.info()

#         exit()
