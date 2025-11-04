import os
import sys
import logging
from datetime import datetime, date
from pprint import pprint

from controldb import ControlDB
from table import Table
from mydb import DataBase
from multipledataBases import MultipleDataBases

mainPath = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
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

    mP = main_path_dec(mainPath)
    sP = sub_path_dec(mainPath)
    # Test Table Class
    testList = []
    testList.append('')                             # 0
    testList.append('ControlDB')                    # 1
    testList.append('Table')                        # 2
    testList.append('DataBase')                     # 3
    testList.append('MultipleDataBases')            # 4
    run = testList[2]

    # Test ControlDB Class
    if run == "Table":
        print(" -> Test main table")
        db = ControlDB()
        engine = db.connect(mP())

        p:Table = Table("main", engine, "MolTable")
        print(p.get_column_names())

        p:Table = Table("main", engine, "LibraryTable")
        print(p.get_column_names())
        print(p.get_item(2, "LibraryCode"))
        print(p.get_id("LibraryCode", "SFF"))
        print(p.get_column_data("LibraryCode"))
        p.info()
        exit()
        # t:Table = p.main_db

    # Test DataBase Class
    if run == "DataBase":
        path = main_path_dec(mainPath)
        p:DataBase = DataBase("root", path, logLevel=logging.DEBUG)
        p.info()

        path = sub_path_dec(mainPath)
        p:DataBase = DataBase("NCN", path, logLevel=logging.DEBUG)
        p.info()
        exit()

    # Test MultipleDataBases Class
    if run == "MultipleDataBases":
        p = MultipleDataBases(mP,sP, "LibraryTable", "LibraryCode", logLevel=logging.INFO)
        t = p.get_table("root", "LibraryTable")
        t.info()

        exit()
