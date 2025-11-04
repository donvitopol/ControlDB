
from src.controldb_manager import ControlDBManager, ControlDB, ROOTBASE
from sqlalchemy import Engine, MetaData

from sqlalchemy import (
    Column,
    Integer,      # Whole numbers
    SmallInteger, # Smaller integers
    BigInteger,   # Large integers
    Float,        # Floating point numbers
    Numeric,      # Exact decimal numbers (e.g., money)
    String,       # Varchar / short text
    Text,         # Long text
    Boolean,      # True/False
    Date,         # Date only
    DateTime,     # Date + time
    Time,         # Time only
    LargeBinary,  # Binary data / files
)
print(__name__)
if __name__ == "__main__":
    models = ROOTBASE
    fullname="Vito Pol"
    username="donvitopol"
    password="test"
    email = "donvitopol@hotmail.com"
    data = {
        "username": "John Doe",
        "fullname": "John Doe",
        "email": "john@example.com"  # ✅ required column
    }  

    cdbm = ControlDBManager(dbName="MyDB", logLevel = 20)
    print(cdbm.rootPath)

    cdbm.setup(username=username, fullname=fullname, password=password, email=email)
    _db = cdbm.create("test_base", password=password, folderSystem=["ik", "ben", "een"], base=models)
    cdbm.create("test_empty_base", password=password, folderSystem=["ik", "ben", "een"], base=MetaData())
    cdbm.create("test_no_base", password=password, folderSystem=["ik", "ben", "een"])
    cdbm.create("test_no_base", password=password)

    # cdbm.login(username, password=password)

    # print(cdbm.databaseDir)
    db:ControlDB = cdbm.get(5)
    columns = {"test": Integer,
               "test1": Integer,
               "test2": Float,
               "test3": String,
               "test4": Boolean,
               "test5": Date}
    
    table1  = db.create_table("FirstTable", columns)
    columns1 = db.get.column_definitions(table1)
    db.create_table("SecondTable", columns1)
    table2 = _db.create_table_from_existing("DatabaseTable")
    columns2 = db.get.column_definitions(table2)
    db.create_table("ThirdTable", columns2)


    close = False
    # close = True
    if close:
        cdbm.remove_all(exec = True)
    else:
        cdbm.detach_all(exec = True)