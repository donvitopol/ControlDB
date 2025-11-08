#!/usr/bin/env python3
"""
ControlDB
===============

Author: Don Vito Pol
Email: don_vito_pol@hotmail.com
Version: 1.0
Created: 2025-09-23

Description:
------------
This module provides tools for managing and interacting with databases.
It supports dynamic table creation, data insertion, updating, and retrieval
using SQLAlchemy, pyodbc, and Excel integration.

Usage:
------
>>> from ControlDB import ControlDB
>>> db = ControlDB("MyDB")
>>> db.connect(password="123")
>>> db.add_row(MyTable, column1='value1', column2=123)
>>> row = db.row(MyTable, 1)
>>> print(row)
>>> db.excel.read_excel("data.xlsx")  # Example Excel interaction
"""

import os, sys, time, gc, stat
import shutil
import inspect
import functools
import pyodbc
import msaccessdb
import urllib
import pandas as pd
import win32com.client

from sqlalchemy import Engine, MetaData, Table, Column, Integer, String
from sqlalchemy import insert, delete, select, text, update
from sqlalchemy import Engine, create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import ProgrammingError
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
from pretty_logger import PrettyLogger
from .utils import UtilsTable, UtilsRow, require_authorization
from .excel_manager import ExcelManager


def construct_folder_path(rootPath: str, folderSystem: str | list[str] = None, levels_up: int = 0) -> str:
    """
    Constructs the absolute path to a database folder.

    Parameters
    ----------
    rootPath : str
        The root directory path.
    folderSystem : str or list of str, optional
        A subfolder or list of subfolders inside the root path.
    levels_up : int, optional
        Number of directories to move up from the root path (default=0).

    Returns
    -------
    str
        Absolute folder path.
    """
    if folderSystem is None:
        return rootPath
    elif isinstance(folderSystem, str):
        return os.path.join(rootPath, folderSystem)
    else:
        return os.path.join(rootPath, *folderSystem)


def construct_file_path(rootPath: str, fileName: str, folderSystem: str | list[str] = None,
                        levels_up: int = 0, db_type: str = "mdb") -> str:
    """
    Constructs the full file path for a database file.

    Parameters
    ----------
    rootPath : str
        The root directory path.
    fileName : str
        Name of the database file (without extension).
    folderSystem : str or list of str, optional
        Subfolder or list of subfolders.
    levels_up : int, optional
        Levels to move up from the root path (default=0).
    db_type : str, optional
        Database extension/type (default='mdb').

    Returns
    -------
    str
        Full path to the database file.
    """
    folderPath = construct_folder_path(rootPath, folderSystem=folderSystem, levels_up=levels_up)
    return os.path.join(folderPath, f"{fileName}.{db_type}")


def construct_name(fileName: str, folderSystem: str | list[str] = None,
                   levels_up: int = 0, db_type: str = "mdb") -> str:
    """
    Constructs a database file name including folder structure.

    Parameters
    ----------
    fileName : str
        Database file name.
    folderSystem : str or list of str, optional
        Folder or list of folders.
    levels_up : int, optional
        Number of levels up (default=0).
    db_type : str, optional
        Database extension (default='mdb').

    Returns
    -------
    str
        Constructed name/path.
    """
    if folderSystem is None:
        return fileName
    elif isinstance(folderSystem, str):
        return os.path.join(folderSystem, fileName)
    else:
        return os.path.join(*folderSystem, fileName)


def remove_folder(path: str, exec: bool = False) -> bool:
    """
    Remove a folder and its contents.

    Parameters
    ----------
    path : str
        Path of the folder to remove.
    exec : bool, optional
        Whether to actually delete (default=False).

    Returns
    -------
    bool
        True if folder removed, False otherwise.
    """
    if not exec or not os.path.exists(path):
        return False
    shutil.rmtree(path)
    return True


class ControlDB():
    """
    ControlDB class for managing MS Access databases and optional Excel integration.

    Attributes
    ----------
    engine : Engine
        SQLAlchemy engine.
    session : Session
        SQLAlchemy session.
    base : MetaData or list
        Metadata or declarative base.
    excel : ExcelManager
        Excel handling class.
    """

    def __init__(self, fileName: str, rootPath: str = None, folderSystem: str | list[str] = None,
                 db_type: str = "mdb", logLevel: int = 30):
        """
        Initialize ControlDB instance.

        Parameters
        ----------
        fileName : str
            Database file name without extension.
        rootPath : str, optional
            Root directory (default=current working directory).
        folderSystem : str or list, optional
            Folder structure inside root.
        db_type : str, optional
            Database extension (mdb/accdb).
        logLevel : int, optional
            Logger level (default=30).
        """
        self.logLevel: PrettyLogger = logLevel
        logCtr = PrettyLogger()
        moduleName: str = "ControlDB"
        self.logger: PrettyLogger = logCtr.get(moduleName)
        logCtr.add_stream(moduleName, level=logLevel)

        # super().__init__()

        self.engine: Engine = None
        self.session: Session = None
        self.base: MetaData | list[MetaData] = None
        self.__id: int = None
        self.__authorized: bool = False
        self.excel = ExcelManager(logLevel=self.logLevel)

        self.__rootPath = rootPath if rootPath else os.getcwd()
        self.__folderSystem:str = os.path.join(*folderSystem) if isinstance(folderSystem, list) else folderSystem
        self.__fileName: str = fileName
        self.__db_type: str = db_type

        self.logger.debug(f'  - {moduleName} -> __init__')
    
    @property
    def id(self) -> int:
        """Database ID."""
        return self.__id

    @id.setter
    def id(self, value: int) -> None:
        """Set the database ID once."""
        if self.__id is not None:
            raise AttributeError("id has already been set and cannot be changed")
        if not isinstance(value, int):
            raise TypeError("id must be an integer")
        if value < 0:
            raise ValueError("id must be non-negative")
        self.__id = value

    @property
    def name(self) -> str:
        """Database name with folder structure."""
        return construct_name(self.__fileName, folderSystem=self.__folderSystem)

    @property
    def rootPath(self) -> str:
        """Root folder path."""
        return self.__rootPath

    @property
    def fileFolder(self) -> str:
        """File folder path."""
        return construct_folder_path(self.rootPath, folderSystem=self.__folderSystem)

    @property
    def folderSystem(self) -> str:
        """Folder system path relative to root."""
        return self.__folderSystem

    @property
    def filePath(self) -> str:
        """Full file path to database."""
        return construct_file_path(self.__rootPath, self.__fileName,
                                   folderSystem=self.__folderSystem, db_type=self.__db_type)

    @property
    def authorized(self) -> bool:
        """Whether user is authorized."""
        return self.__authorized

    @staticmethod
    def get_folders(path: str) -> list[str]:
        """Return list of folders in a directory."""
        return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]

    def __set_mdb_password(self, filepath: str, password: str) -> None:
        """
        Set or change the password for an MS Access database using DAO COM.

        Parameters
        ----------
        filepath : str
            Full path to the database file.
        password : str
            New password to set.

        Returns
        -------
        None

        Example
        -------
        >>> db.__set_mdb_password("MyDB.mdb", "newpassword123")
        """
        engine = win32com.client.Dispatch("DAO.DBEngine.120")
        # Open in EXCLUSIVE mode: (Exclusive=True, ReadOnly=False)
        db = engine.OpenDatabase(filepath, True, False, ";PWD=")
        db.NewPassword("", password)
        db.Close()
        self.logger.info("   -> Password set via COM")

    def __create_path(self) -> str:
        """
        Ensure the folder path exists for the database, creating it if necessary.

        Returns
        -------
        str
            Absolute folder path.

        Example
        -------
        >>> folder_path = db.__create_path()
        >>> print(folder_path)
        """
        self.logger.info("   -> Create Folder Path")
        self.logger.debug(f"     => Root Path:      {self.rootPath}")
        self.logger.debug(f"     => File Folder:    {self.fileFolder}")
        self.logger.debug(f"     => File Path:      {self.filePath}")

        if not os.path.exists(self.rootPath):
            os.makedirs(self.rootPath, exist_ok=True)
            self.logger.info(f"     => Root Path created.")

        if not os.path.exists(self.fileFolder):
            os.makedirs(self.fileFolder, exist_ok=True)
            self.logger.info(f"     => Folder Path created.")
        else:
            self.logger.info(f"     => Folder Path present.")

        return self.rootPath

    def create_file(self, password: str = "") -> bool:
        """
        Create an MS Access database file if it does not exist, and set a password if provided.

        Parameters
        ----------
        password : str, optional
            Password to secure the database file (default="").

        Returns
        -------
        bool
            True if file was created, False if file already exists.

        Example
        -------
        >>> created = db.create_file(password="mypassword")
        >>> print(created)
        """
        # Ensure folder exists
        self.__create_path()

        fileNameOnly = os.path.basename(self.filePath)
        self.logger.info(f"   -> Create File {fileNameOnly}")
        self.logger.debug(f"     => File Path: {self.filePath}")
        

        if not os.path.exists(self.filePath):
            msaccessdb.create(self.filePath)
            self.__set_mdb_password(self.filePath, password)
            self.logger.info("     => File created.")
            return True
        else:
            self.logger.info("     => File present.")
            return False

    def setup(self, password: str = "", base: MetaData | list[MetaData] = None, login: bool = True) -> bool:
        """
        Create the database file and initialize tables if they do not exist.

        Parameters
        password : str, optional
            Password for the database (default="").
        base : MetaData or list[MetaData], optional
            SQLAlchemy MetaData objects or declarative bases to create tables.
        login : bool, optional
            Whether to stay connected after setup (default True).

        Returns
        -------
        bool
            True if setup was completed successfully.

        Raises
        ------
        ValueError
            If SQLAlchemy engine is not initialized.

        Example
        -------
        >>> db.setup(password="mypassword", base=[Base])
        True
        """
        self.create_file(password=password)
        self.connect(password=password, base=base)

        if self.engine is None:
            raise ValueError("[ERROR] SQLAlchemy engine is None — cannot proceed.")

        self.base = base if base is not None else self.base

        if self.base:
            bases = self.base if isinstance(self.base, list) else [self.base]
            for b in bases:
                if hasattr(b, "metadata"):
                    b.metadata.create_all(bind=self.engine)
                else:
                    b.create_all(bind=self.engine)
        else:
            self.logger.warning("ℹ️ No base provided — skipping table creation.")

        if not login:
            self.detach()

        return True

    def connect(self, password: str = "", base: MetaData | list[MetaData] = None) -> bool:
        """
        Connect to an MS Access database and initialize SQLAlchemy session.

        Parameters
        ----------
        password : str, optional
            Database password (default="").
        base : MetaData or list[MetaData], optional
            Metadata or declarative bases for table creation.

        Returns
        -------
        bool
            True if connection succeeded.

        Raises
        ------
        FileNotFoundError
            If the database file does not exist.
        ConnectionRefusedError
            If no valid MS Access ODBC driver is installed.
        pyodbc.Error
            If connection attempt fails.

        Example
        -------
        >>> connected = db.connect(password="mypassword", base=[Base])
        >>> print(connected)
        True
        """
        if not os.path.exists(self.filePath):
            self.logger.error(f"    ❌ - Database file not found: {self.filePath}")
            raise FileNotFoundError(f"Database file does not exist: {self.filePath}")

        msa_drivers = [x for x in pyodbc.drivers() if "ACCESS" in x.upper()]
        if "Microsoft Access Driver (*.mdb, *.accdb)" not in msa_drivers:
            self.logger.critical("=! Wrong engine installed. Please install 64-bit MS Access Driver.")
            self.logger.info(f"   => Available MS-ACCESS Drivers: {msa_drivers}")
            raise ConnectionRefusedError("No valid MS Access ODBC driver found!")

        try:
            con_parts = [r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};", f"DBQ={self.filePath};"]
            ext = os.path.splitext(self.filePath)[1][1:]
            if ext == "mdb":
                con_parts.append(f"Uid=Admin;Pwd={password};" if password else "Uid=Admin;Pwd=;")
            else:
                if password:
                    con_parts.append(f"Pwd={password};")
                else:
                    self.logger.warning(" -> ACCDB database not password protected")

            con_string = "".join(con_parts)
            connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(con_string)}"

            self.engine = create_engine(connection_url)
            with self.engine.connect():
                pass
        except pyodbc.Error as e:
            if "Not a valid password" in str(e):
                self.logger.warning("⛔ Login attempt failed: Invalid password for database.")
                self.__authorized = False
                return False
            else:
                self.logger.error("❌ Failed to connect to Access DB.", exc_info=True)
                self.__authorized = False
                raise

        self.__authorized = True
        self.base = base if base else MetaData()
        _Session = sessionmaker(bind=self.engine)
        self.session = _Session()

        self.logger.info(f"   -> Connect to database: {self.name} => Connection established successfully.")
        self.logger.debug(f"     => File path of database: {self.filePath}")

        return True
    
    def detach(self)->None:
        try:
            if hasattr(self, "session") and self.session:
                self.session.close()
                self.session = None
                self.logger.debug(" -> Session closed.")

            if self.engine:
                self.engine.dispose()
                self.engine = None
                self.logger.debug(" -> Engine disposed.")

            gc.collect()
            
            # Mark as closed authorized
            self.__authorized = False
            time.sleep(0.4)  # Let OS flush handles
        except Exception as e:
            self.logger.error(f"Error while fully closing database: {e}")
  
    def remove_folder(self, exec: bool = False, retries: int = 5, delay: float = 0.5) -> bool:
        """
        Safely delete the database folder, but only if the database file is already removed.

        Parameters
        ----------
        exec : bool
            Whether to actually execute the deletion (default=False).
        retries : int
            Number of retry attempts for deletion.
        delay : float
            Delay between retries in seconds.

        Returns
        -------
        bool
            True if folder was successfully removed, False otherwise.
        """
        # Check if database file still exists
        if os.path.exists(getattr(self, "filePath", "")):
            self.logger.error(f"Cannot remove folder because database file still exists: {self.filePath}")
            return False

        if not exec:
            self.logger.info(f"[DRYRUN] Would remove folder: {self.rootPath}")
            return False

        if not os.path.exists(self.rootPath):
            self.logger.warning(f"⚠️ Folder does not exist: {self.rootPath}")
            return False

        def _handle_remove_readonly(func, path, _):
            """Helper to change file permission and retry deletion."""
            try:
                os.chmod(path, stat.S_IWRITE)
                func(path)
            except Exception as e:
                self.logger.warning(f"⚠️ Could not remove {path}: {e}")

        for attempt in range(1, retries + 1):
            try:
                shutil.rmtree(self.rootPath, onerror=_handle_remove_readonly)
                self.logger.info(f"✅ Folder successfully removed: {self.rootPath}")
                return True
            except PermissionError as e:
                self.logger.warning(f"⛔ Permission denied (attempt {attempt}/{retries}): {e}")
            except OSError as e:
                self.logger.warning(f"⚠️ OS error during remove (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)

        self.logger.error(f"❌ Could not remove folder after {retries} attempts: {self.rootPath}")
        return False
  
    @require_authorization
    def remove(self, exec: bool = False, removeFolder: bool = True, retries: int = 5, delay: float = 0.5) -> bool:
        """
        Safely delete the database file, handling locked files and retrying if necessary.

        Parameters
        ----------
        exec : bool, optional
            Whether to actually execute the deletion (default=False).
        removeFolder : bool, optional
            Whether to remove associated folder (not used in this function, reserved for future use, default=True).
        retries : int, optional
            Number of retry attempts if the file is locked (default=5).
        delay : float, optional
            Delay in seconds between retries (default=0.5).

        Returns
        -------
        bool
            True if the database file was successfully removed, False otherwise.

        Raises
        ------
        PermissionError
            If the file could not be removed after the given number of retries.

        Example
        -------
        >>> db.remove(exec=True)
        True
        """
        self.logger.debug(f"  - ControlDB -> remove: {self.filePath}")

        if not exec or not os.path.isfile(self.filePath):
            return False

        for attempt in range(1, retries + 1):
            self.logger.debug("  -> Closing sessions and disposing engine...")
            try:
                self.detach()
            except Exception as e:
                self.logger.warning(f"  -> Could not fully close connections: {e}")

            ldb_file = self.filePath[:-3] + "ldb"

            try:
                if os.path.exists(ldb_file):
                    self.logger.warning(f" -> LDB lock file exists: {ldb_file}")
                    # Optional: os.remove(ldb_file)  # only if safe
                os.remove(self.filePath)
                self.logger.info(f"    ✅ - Database file successfully removed: {self.filePath}")
                return True
            except PermissionError:
                self.logger.warning(f"    ⛔ - Retry {attempt}/{retries}")
                time.sleep(delay)

        raise PermissionError(
            f"Could not remove file after {retries} retries: {self.filePath}. "
            f"Close any applications using it (Access, DAO, etc.)"
        )

    @require_authorization
    def create_table(self, table_name: str, columns: dict, metadata: MetaData = None) -> Table:
        """
        Dynamically create a new SQLAlchemy table in the connected database.

        The function accepts different ID column name formats ('id', 'Id', 'ID'),
        but the final database column will always be created as 'ID'
        (auto-incrementing primary key).

        Args:
            table_name (str): Name of the table to create.
            columns (dict): Mapping of column names to SQLAlchemy types.
            metadata (MetaData, optional): SQLAlchemy MetaData object.

        Returns:
            Table: The created SQLAlchemy Table object.
        """
        if not self.engine:
            raise RuntimeError("⛔ - No active engine connection")

        # Use existing metadata or create new
        if metadata is None:
            metadata = MetaData()

        # ✅ Normalize ID column key (accept 'id', 'Id', 'ID')
        normalized_columns = {}
        has_id = False
        for name, col_type in columns.items():
            if name.lower() == "id":
                normalized_columns["ID"] = col_type
                has_id = True
            else:
                normalized_columns[name] = col_type

        # ✅ Ensure ID column exists if missing
        if not has_id:
            normalized_columns = {"ID": Integer} | normalized_columns

        # ✅ Build column objects
        column_objs = []
        for name, col_type in normalized_columns.items():
            if name == "ID":
                column_objs.append(Column("ID", Integer, primary_key=True, autoincrement=True))
            else:
                column_objs.append(Column(name, col_type))

        # ✅ Create and commit table
        table = Table(table_name, metadata, *column_objs)
        metadata.create_all(self.engine)

        self.logger.info(f"✅ - Table '{table_name}' created successfully with standardized ID column")
        return table

    @require_authorization
    def load_table(self, table_identity: any) -> UtilsTable:
        """
        Create a UtilsTable object from an existing database table.

        Args:
            table_identity (str | object): Existing table name (str) or ORM Table/Class (object).
        Returns:
            UtilsTable: Wrapped UtilsTable instance for the existing table.
        """
        def create_core_table() -> Table:
            meta = MetaData()  # altijd nieuw MetaData object
            with self.engine.connect() as conn:
                stmt = text(f"SELECT * FROM [{table_identity}] WHERE 1=0")
                result = conn.execute(stmt)
                column_names = result.keys()
            columns = [
                Column(name, Integer, primary_key=True, autoincrement=True) if name.lower() == "id" else Column(name, String)
                for name in column_names
            ]
            return Table(table_identity, meta, *columns)
        
        if isinstance(table_identity, str):
            core_table = create_core_table()
            uTable = UtilsTable(core_table, engine=self.engine, session=self.session, base=self.base, logLevel=self.logLevel)
        else:
            # ORM class of Table object
            uTable = UtilsTable(table_identity, engine=self.engine, session=self.session, base=self.base, logLevel=self.logLevel)

        self.logger.debug(f" => Table '{table_identity}' mapped from existing database")
        return uTable
    
    @require_authorization
    def get_table_names(self) -> list[str]:
        """Return a list of non-system table names."""
        try:
            with self.engine.connect() as conn:
                cursor = conn.connection.cursor()
                names = [n.table_name for n in cursor.tables() if "MSys" not in n.table_name]
                return names
        except Exception as e:
            self.logger.warning(f"⚠️ - Could not fetch table names: {e}")
            return []
        