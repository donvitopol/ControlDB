
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Table, Column
from sqlalchemy import Integer, Numeric, String, Boolean, Float

ROOTBASE = declarative_base()

class UserTable(ROOTBASE):
    __tablename__ = "UserTable"
    __table_args__ = {'extend_existing': True}
    ID =            Column(Integer, primary_key=True, autoincrement=True)
    username =      Column(String,  unique=True,  nullable=False)
    password =      Column(String)
    fullname =      Column(String)
    email =         Column(String)

    def __init__(self, username, password, fullname, email):
        self.username = username
        self.password = password
        self.fullname = fullname
        self.email = email

    def __repr__(self):
        return f"{self.fullname}, {self.username}, {self.email}, {self.password}"


class DatabaseTable(ROOTBASE):
    __tablename__ = "DatabaseTable"
    __table_args__ = {'extend_existing': True}

    ID = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    rootPath = Column(String)
    fileName = Column(String)
    folderSystem = Column(String)
    db_type = Column(String)
    base = Column(String)
    logLevel = Column(Integer)

    def __init__(
        self,
        name: str,
        rootPath: str = None,
        fileName: str = None,
        folderSystem: str = None,
        db_type: str = None,
        base: str = None,
        logLevel: int = 30
    ):
        """Initialize all DatabaseTable attributes."""
        self.name = name
        self.rootPath = rootPath
        self.fileName = fileName
        self.folderSystem = folderSystem
        self.db_type = db_type
        self.base = str(base) if base is not None else None
        self.logLevel = logLevel

    def __repr__(self) -> str:
        """Return a detailed string representation for debugging or logging."""
        return (
            f"<DatabaseTable("
            f"ID={self.ID}, "
            f"name='{self.name}', "
            f"rootPath='{self.rootPath}', "
            f"fileName='{self.fileName}', "
            f"folderSystem='{self.folderSystem}', "
            f"db_type='{self.db_type}', "
            f"base='{self.base}', "
            f"logLevel={self.logLevel})>"
        )