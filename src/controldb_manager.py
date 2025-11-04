import os, sys, time, gc
from pretty_logger import PrettyLogger, prettylog
from sqlalchemy import Engine, create_engine, MetaData
from sqlalchemy.exc import IntegrityError  # Assuming SQLAlchemy is used

from .controldb import ControlDB, remove_folder, construct_folder_path, construct_file_path, require_authorization
from .models.root import ROOTBASE, UserTable, DatabaseTable


@prettylog
class ControlDBManager():
    @property
    def name(self)->int:          
        return self.__dbName
    
    @property
    def rootPath(self)->int:          
        return self.__rootPath
    
    @property
    def userName(self)->dict:          
        return self.__userName    
    
    @property
    def authorized(self)->dict:          
        return self.__authorized   
    
    def __init__(self, dbName:str="database", rootPath:str=None, db_type: str = "mdb", logLevel: int = 30):    
        self.__dbName:str= dbName  
        self.__db_type:str= db_type   
        self.logLevel:int= logLevel   
        self.logger:PrettyLogger 
         
        self.__userName:str
        self.__password:str
        self.__authorized:bool=False
        self.__setup:bool=False

        # Get the absolute path of the current file
        if not rootPath:
            rootPath = os.getcwd()

        self.__rootPath = construct_folder_path(rootPath, folderSystem=dbName)

        self.databaseDir:dict[int:ControlDB]={}    
     
    def __create(self, fileName, password="", folderSystem: str = None, base: MetaData|list[MetaData] = None)->ControlDB:

        db = ControlDB(fileName, rootPath=self.rootPath, folderSystem=folderSystem, db_type=self.__db_type, logLevel=self.logLevel)
        db.setup(password=password, base=base)
        
        dbRoot:ControlDB = self.databaseDir.get(1)
        if dbRoot:
            dbExec = dbRoot
        else:
            dbExec = db

        id = dbExec.row_create(DatabaseTable, name=db.name, rootPath=self.rootPath, 
                               folderSystem=folderSystem, fileName=fileName, db_type=self.__db_type, 
                               base=str(base) if base is not None else None, 
                               logLevel=self.logLevel)
        
        db.id = id
        return db
    
    def setup(self, username="admin", fullname="", password="", email="admin@example.com"):
        """
        Sets up the root database and root user if they do not already exist.

        Parameters:
            username (str): Username for the root account. Default is 'admin'.
            fullname (str): Full name for the root account.
            password (str): Password for the root account. Default is empty string.
            email (str): Email for the root account. Default is 'admin@example.com'.

        Returns:
            str: Path to the database directory if setup is skipped or completed.
        """

        # Construct the path to the root database file
        root_file_path = os.path.join(self.rootPath, "root.db")  # adjust to your actual root file name

        # Check if the root folder and root database file already exist
        if os.path.isdir(self.rootPath) and os.path.isfile(root_file_path):
            self.logger.warning("⚠️ Root database already exists — skipping setup.")
            return self.rootPath  # Skip setup if database already exists

        # Ensure the root database folder exists (create it if necessary)
        os.makedirs(self.rootPath, exist_ok=True)

        # Create the database (this also initializes internal tables)
        db: ControlDB = self.__create("root", password=password, base=ROOTBASE)
        self.__setup = True  # Flag that setup is in progress

        # Login with the root user
        self.login(username, password=password)

        # Attempt to create the root user row in the UserTable
        try:
            db.row_create(UserTable, username=username, fullname=fullname, password=password, email=email)
            self.logger.info(f"✅ Root user '{username}' created successfully.")
        except IntegrityError:
            # This occurs if the username already exists due to the unique constraint
            self.logger.info(f"ℹ️ Root user '{username}' already exists — skipping creation.")

        # Reset the setup flag
        self.__setup = False

        # Return the path to the root database folder
        return self.rootPath

    def login(self, userName: str, password: str = "") -> bool:
        """
        Authenticate a user and load their associated databases.

        The method attempts to verify user credentials against the `UserTable`
        in the root database. On successful authentication, all databases
        registered in `DatabaseTable` are loaded.

        Parameters
        ----------
        userName : str
            The username attempting to log in.
        password : str, optional
            The corresponding user password. Default is an empty string.

        Returns
        -------
        bool
            True if the user is successfully authenticated and databases are loaded,
            False otherwise.

        Raises
        ------
        NameError
            If the username does not exist in the `UserTable`.
        ValueError
            If the username or password is incorrect.
        Exception
            For any other database or connection-related errors.
        """
        self.logger.info(f" -> Login attempt: '{userName}'.")

        try:         
            self.__authorized = True

            # During setup, only connect to root
            if self.__setup:
                self.__userName = userName
                self.__password = password       
                self.__load_root(password=password)
                return True

            # Load root DB for user validation
            dbRoot = self.__load_root(password=password)

            # Validate user in UserTable
            userIDs = dbRoot.get.ids(UserTable, "username", userName)
            if not userIDs:
                raise NameError(f"User '{userName}' not found in UserTable")

            userID = userIDs[0]
            row = dbRoot.get.row(UserTable, userID)

            # Explicit validation of both username and password
            if row["username"] != userName:
                raise ValueError(f"Username mismatch: expected '{userName}', got '{row['username']}'")
            if row["password"] != password:
                raise ValueError("Incorrect password")

            # Authorized — load 
            self.__authorized = True
            self.__load(password=password)
            self.__userName = userName
            self.__password = password
            self.logger.info("✅ Authorized")
            return True

        except (NameError, ValueError) as e:
            self.__authorized = False
            self.logger.warning(f"⛔ Login failed: {e}")
            return False

        except Exception as e:
            self.__authorized = False
            self.logger.error(f"⛔ Unexpected authorization error: {e}")
            return False
         
    @require_authorization
    def __load_root(self, password: str = "") -> ControlDB:
        """
        Load or connect to the root ControlDB instance.

        If the root database is already loaded in `self.databaseDir`, it will
        simply return that instance. Otherwise, it creates and connects a new
        `ControlDB` object pointing to the root database file.

        Parameters
        ----------
        password : str, optional
            Password used to open or connect to the root database.

        Returns
        -------
        ControlDB
            The connected root ControlDB instance.

        Raises
        ------
        Exception
            If connection to the root database fails.
        """
        if 1 in self.databaseDir:
            return self.databaseDir[1]

        dbRoot = ControlDB(
            "root",
            rootPath=self.rootPath,
            db_type=self.__db_type,
            logLevel=self.logLevel
        )
        dbRoot.id = 1
        dbRoot.connect(password=password)
        self.databaseDir[dbRoot.id] = dbRoot
        return dbRoot

    @require_authorization
    def __load(self, password: str = "") -> None:
        """
        Load all registered databases defined in the root `DatabaseTable`.

        This method queries the `DatabaseTable` in the root database to retrieve
        all existing database entries and connects them into the manager's directory.

        Parameters
        ----------
        password : str, optional
            The password used to connect each database.

        Returns
        -------
        None

        Raises
        ------
        Exception
            If a database entry cannot be connected or instantiated.
        """
        dbRoot = self.__load_root(password=password)

        for id in dbRoot.get.column_as_list(DatabaseTable, "ID"):
            if id == 1:
                continue  # Skip root itself

            row = dbRoot.get.row(DatabaseTable, id)
            dbX = ControlDB(
                row["fileName"],
                rootPath=row["rootPath"],
                folderSystem=row["folderSystem"],
                db_type=row["db_type"],
                logLevel=row["logLevel"]
            )
            dbX.connect(password=password, base=row["base"])
            dbX.id = id
            self.databaseDir[id] = dbX
       
    @require_authorization
    def create(self, *args, **kwargs) -> ControlDB:
        """Create a new ControlDB instance and store it in databaseDir."""
        folderSystem = kwargs.get("folderSystem")
        if not folderSystem:
            kwargs["folderSystem"] = None
        elif isinstance(folderSystem, list):
            kwargs["folderSystem"] = os.path.join(*folderSystem)

        db: ControlDB = self.__create(*args, **kwargs)
        self.databaseDir[db.id] = db
        self.logger.info(f" => Database created with ID {db.id} at {kwargs.get('folderSystem')}")
        return db
        
    @require_authorization
    def get(self, indentity: int | str) -> ControlDB|None:
        # If 'indentity' is an integer, treat it as the database ID
        if isinstance(indentity, int):
            return self.databaseDir.get(indentity, None)

        # If 'indentity' is a string, treat it as the database name
        for db in self.databaseDir:
            if db.name == indentity:
                return db

        # Return None if no matching database is found
        return None
        
    @require_authorization
    def detach_all(self, exec: bool = False, retries: int = 5, delay: float = 0.5): 
        self.logger.info("⚠️  Detach all databases")
        db:ControlDB
        for db in self.databaseDir.values():
            self.logger.info(f" -> Detach Database: {db.name}")
            db.detach()

    @require_authorization
    def remove_all(self, exec: bool = False, retries: int = 5, delay: float = 0.5): 
        self.logger.warning("⛔ - Remove all databases")
        db:ControlDB
        for db in self.databaseDir.values():
            self.logger.info(f" -> Remove Database: {db.name}")
            db.remove(exec = exec, retries = retries, delay = delay)
        
        remove_folder(self.rootPath ,exec = exec)
