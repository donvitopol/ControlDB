import pandas as pd
import functools
from sqlalchemy import Engine, MetaData, Table, Column, Integer, String
from sqlalchemy import insert, delete, select, text, update
from sqlalchemy.orm import sessionmaker, Session, DeclarativeMeta
from pretty_logger import PrettyLogger, prettylog
from sqlalchemy import update

from src import UtilsRow

def require_authorization(func):
    """Decorator to enforce authorization before method execution."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not getattr(self, "authorized", False):
            raise PermissionError(f"⛔ - Unauthorized access to '{func.__name__}'")
        return func(self, *args, **kwargs)
    return wrapper


@prettylog
class UtilGetManager:
    """Helper class for retrieving rows, columns, and metadata from database tables."""

    def __init__(self, logLevel: int = 30):
        self.logger: PrettyLogger
        self.table_mapping: dict = None
        self.base: MetaData | list[MetaData] = None
        self.engine: Engine
        self.session: Session
        self.logLevel = logLevel
        self._authorized = True

    @property
    def authorized(self) -> bool:
        """Check if the manager is authorized."""
        return getattr(self, "_authorized", True)

    def connect(self, engine: Engine, session: Session, base: MetaData | list[MetaData]):
        """
        Connect the manager to a database engine and session.

        Args:
            engine (Engine): SQLAlchemy engine.
            session (Session): SQLAlchemy session.
            base (MetaData | list[MetaData]): SQLAlchemy MetaData object(s).
        """
        self.engine = engine
        self.session = session
        self.base = base
        self.logger.debug(" -> UtilGetManager connected")

    def get_first_free_id(self, table_or_class, pk: str = "ID") -> int:
        """
        Find the first free ID in a table. Returns max(id)+1 if no gaps.

        Args:
            table_or_class (object): ORM class or Core Table.
            pk (str): Primary key column name.

        Returns:
            int: First available ID, or -1 if error.
        """
        try:
            ids = [row[0] for row in self.session.execute(select(getattr(table_or_class, pk))).all()]
            ids = sorted([i for i in ids if i is not None])

            if not ids:
                self.logger.debug(f" => Table '{table_or_class.__tablename__}' is empty, returning ID=1")
                return 1

            for expected, actual in enumerate(ids, start=1):
                if expected != actual:
                    self.logger.debug(f" => First free id in '{table_or_class.__tablename__}': {expected}")
                    return expected

            next_id = ids[-1] + 1
            self.logger.debug(f" => No gaps found, next ID={next_id}")
            return next_id

        except Exception as e:
            self.logger.error(f"❌ - Error finding first free ID in '{table_or_class}': {e}")
            return -1
    
    def row_control(self, table_class: object, row_id: int = None) -> "UtilsRow":
        """
        Initialize a UtilsRow instance for a given table_class and optional row_id.

        Args:
            table_class (object): ORM class or Core Table.
            row_id (int, optional): Optional ID of the row to target.

        Returns:
            UtilsRow: A UtilsRow instance connected to this table and row.
        """
        if table_class is None:
            self.logger.warning("⚠️ - table_class is None, cannot create UtilsRow")
            return None

        utils_row = UtilsRow(table_class=table_class, row_id=row_id, logLevel=self.logLevel)
        
        # Connect the UtilsRow instance to the same engine/session/base
        if hasattr(self, "engine") and hasattr(self, "session") and hasattr(self, "base"):
            utils_row.connect(engine=self.engine, session=self.session, base=self.base)
        else:
            self.logger.warning("⚠️ - UtilGetManager is not connected, UtilsRow not connected yet")

        return utils_row

    def row(self, table_class: object, id: int) -> dict | None:
        """
        Fetch a single row by ID from ORM class or Core Table.

        Args:
            table_class (object): ORM class or Table.
            id (int): ID to fetch.

        Returns:
            dict | None: Row data as dict, or None if not found.
        """
        if table_class is None:
            raise ValueError("⛔ - Table class cannot be None")

        # Determine filter expression for ID column
        filter_expr = table_class.c.ID if hasattr(table_class, "c") else table_class.ID

        # Choose query style based on whether it's ORM or Core
        if hasattr(table_class, "__table__"):  # ORM
            result = self.session.query(table_class).filter(filter_expr == id).first()
        else:  # Core table
            result = self.session.execute(
                table_class.select().where(filter_expr == id)
            ).first()

        if not result:
            return None

        # Convert result to dict
        if hasattr(result, "_mapping"):
            return dict(result._mapping)

        if hasattr(result, "__table__"):
            # ORM instance → extract column data
            return {col.name: getattr(result, col.name) for col in result.__table__.columns}

        # Fallback for other result types
        return {k: v for k, v in vars(result).items() if not k.startswith("_")}

    def column_as_list(self, table_or_class: object, column_name: str) -> list:
        """
        Retrieve all values from a single column.

        Args:
            table_or_class (object): ORM class or Core Table.
            column_name (str): Column name to fetch.

        Returns:
            list: List of values in the column.
        """
        if not table_or_class:
            raise ValueError("⛔ - Table object or ORM class cannot be None")

        # ORM class
        if hasattr(table_or_class, "__table__"):
            column = getattr(table_or_class, column_name, None)
            if column is None:
                raise ValueError(f"⛔ - Column '{column_name}' not found in ORM table '{table_or_class.__tablename__}'")
            rows = self.session.query(column).all()
            return [row[0] for row in rows]

        # Core Table
        if hasattr(table_or_class, "columns"):
            column = table_or_class.columns.get(column_name)
            if column is None:
                raise ValueError(f"⛔ - Column '{column_name}' not found in Core table '{table_or_class.name}'")
            with self.engine.connect() as conn:
                result = conn.execute(select(column))
                return [row[0] for row in result]

        raise ValueError("⛔ - Input must be ORM class or Core Table")

    def column_names(self, table_or_class: object) -> list[str]:
        """
        Return all column names.

        Args:
            table_or_class (object): ORM class or Core Table.

        Returns:
            list[str]: Column names.
        """
        if hasattr(table_or_class, "columns"):
            return [col.name for col in table_or_class.columns]
        if hasattr(table_or_class, "__table__"):
            return [col.name for col in table_or_class.__table__.columns]
        raise ValueError("⛔ - Input must be ORM class or Core Table")

    def column_definitions(self, table_or_class: object) -> dict[str, type]:
        """
        Return a dict of column_name -> column_type.

        Args:
            table_or_class (object): ORM class or Core Table.

        Returns:
            dict[str, type]: Mapping column_name to SQLAlchemy type.
        """
        if hasattr(table_or_class, "columns"):
            return {col.name: type(col.type) for col in table_or_class.columns}
        if hasattr(table_or_class, "__table__"):
            return {col.name: type(col.type) for col in table_or_class.__table__.columns}
        raise ValueError("⛔ - Input must be ORM class or Core Table")

    def df_table(self, table_or_class: object) -> pd.DataFrame | None:
        """
        Return all table data as pandas DataFrame.

        Args:
            table_or_class (object): ORM class or Core Table.

        Returns:
            pd.DataFrame | None: DataFrame with ID as index, or None if error.
        """
        table_name = getattr(table_or_class, "__tablename__", None) or getattr(table_or_class, "name", None)
        if not table_name:
            self.logger.warning("⚠️ - Get Table: INVALID table object")
            return None

        sql = f'SELECT * FROM [{table_name}]'
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(sql, conn)
                if "ID" in df.columns:
                    df = df.set_index("ID", drop=True)
                self.logger.debug(f" => DataFrame fetched from '{table_name}'")
                return df
        except Exception as e:
            self.logger.warning(f"⚠️ - Get Table: FAILED - {e}")
            return None



# ---------------- UtilManager ---------------- #
class UtilManager:
    """Main utility manager class handling database actions and row/column operations."""

    def __init__(self) -> None:
        """Initialize the UtilManager with PrettyLogger and internal UtilGetManager."""
        logCtr = PrettyLogger()
        moduleName = "UtilManager"
        self.logger: PrettyLogger = logCtr.get(moduleName)
        self.logLevel:int
        logCtr.add_stream(moduleName, level=self.logLevel)
        self.logger.debug("  - UtilManager -> __init__")

        # Database attributes
        self.base: MetaData | list[MetaData] = None
        self.engine: Engine = None
        self.engine: Engine = None
        self.session: Session = None

        # Utility getter manager
        self.get = UtilGetManager(logLevel=30)

    @require_authorization
    def connect(self) -> None:
        """
        Connect the manager to a database engine, session, and base metadata.

        Returns:
            None
        """
        self.get.connect(self.engine, self.session, self.base)
        self.logger.debug(" -> UtilManager connected")

    @require_authorization
    def update_column_value(self, table_class: object, row_id: int, column_name: str, new_value) -> bool:
        """
        Update a single column value for a given row.

        Args:
            table_class (object): ORM class of the table.
            row_id (int): ID of the row to update.
            column_name (str): Column name to update.
            new_value: New value to assign.

        Returns:
            bool: True if update succeeded, else raises exception.
        """
        if not table_class:
            raise ValueError("⛔ - Table not provided")
        column = getattr(table_class, column_name, None)
        if not column:
            raise ValueError(f"⛔ - Column '{column_name}' not found")
        row = self.session.query(table_class).filter(table_class.ID == row_id).first()
        if not row:
            raise ValueError(f"⛔ - Row with ID={row_id} not found")
        setattr(row, column_name, new_value)
        self.session.commit()
        self.logger.debug(f" => Row {row_id} updated column '{column_name}'")
        return True
    
    @require_authorization
    def table_names(self) -> list[str]:
        """
        Return a list of table names in the database.

        Returns:
            list[str]: Table names.
        """
        with self.engine.connect() as conn:
            try:
                cursor = conn.connection.cursor()
                tableNames = [name.table_name for name in cursor.tables() if "MSys" not in name.table_name]
                self.logger.debug(f" => Table names fetched: {tableNames}")
                return tableNames
            except Exception as e:
                self.logger.warning(f"⚠️ - Get table names failed: {e}")
                return []

    @require_authorization
    def add_column(self, table_class: object, column_name: str, column_type):
        """
        Add a single column to a table.

        Args:
            table_class (object): ORM class.
            column_name (str): Column name.
            column_type: SQLAlchemy column type (Integer, String, etc.).
        """
        if not table_class:
            raise ValueError("⛔ - Table class missing")
        table_name = table_class.__tablename__
        type_name = column_type.__visit_name__
        sql = f'ALTER TABLE {table_name} ADD COLUMN {column_name} {type_name}'
        self.session.execute(sql)
        self.session.commit()
        self.logger.debug(f" => Column '{column_name}' added to '{table_name}'")

    @require_authorization
    def add_columns(self, table_class: object, columns: dict):
        """
        Add multiple columns to a table.

        Args:
            table_class (object): ORM class.
            columns (dict): {column_name: column_type}.
        """
        if not table_class:
            raise ValueError("⛔ - Table class missing")
        table_name = table_class.__tablename__
        try:
            for col_name, col_type in columns.items():
                type_name = col_type().compile(dialect=self.engine.dialect)
                sql = f'ALTER TABLE {table_name} ADD COLUMN {col_name} {type_name}'
                self.session.execute(sql)
                self.logger.debug(f" => Column '{col_name}' added to '{table_name}'")
            self.session.commit()
            self.logger.info(f"✅ - Columns {list(columns.keys())} added to '{table_name}'")
        except Exception as e:
            self.session.rollback()
            self.logger.warning(f"⚠️ - Add columns failed: {e}")
            raise

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
    def load_row_existing_table(self, table_name: str, metadata: MetaData = None) -> Table:
        """
        Create a Table object from an existing database table.

        Args:
            table_name (str): Existing table name.
            metadata (MetaData): Optional MetaData object.

        Returns:
            Table: SQLAlchemy Table object.
        """
        with self.engine.connect() as conn:
            stmt = text(f"SELECT * FROM [{table_name}] WHERE 1=0")
            result = conn.execute(stmt)
            column_names = result.keys()
        if metadata is None:
            metadata = MetaData()
        columns = []
        for name in column_names:
            if name.lower() == "id":
                columns.append(Column(name, Integer, primary_key=True, autoincrement=True))
            else:
                columns.append(Column(name, String))
        table = Table(table_name, metadata, *columns)
        self.logger.debug(f" => Table '{table_name}' mapped from existing database")
        return table

    
    # @require_authorization
    # def row_create(self, table_or_class: object, *args, **kwargs) -> int | None:
    #     """
    #     Insert a new row into an ORM class or Core Table.

    #     Returns:
    #         int | None: The inserted row ID if available, else None.
    #     """
    #     if table_or_class is None:
    #         self.logger.warning("⚠️ - Table/class not provided")
    #         return None

    #     try:
    #         # --- ORM TABLE ---
    #         if hasattr(table_or_class, "__table__"):
    #             row = table_or_class(*args, **kwargs)
    #             self.session.add(row)
    #             self.session.commit()
    #             self.logger.info(f"✅ ORM row added to '{table_or_class.__tablename__}'")
    #             return getattr(row, "ID", getattr(row, "id", None))

    #         # --- CORE TABLE ---
    #         elif hasattr(table_or_class, "columns"):
    #             # Detect correct ID column name (case-insensitive)
    #             id_col = next((name for name in ("ID", "id", "Id") if name in table_or_class.columns), None)

    #             stmt = insert(table_or_class).values(**kwargs)
    #             inserted_id = None
    #             with self.engine.begin() as conn:
    #                 if "access" in str(self.engine.url).lower():
    #                     conn.execute(stmt)
    #                     result = conn.execute(text("SELECT @@IDENTITY AS last_id"))
    #                     inserted_id = result.scalar()
    #                 else:
    #                     pk_col = next((c for c in table_or_class.columns if c.primary_key), None)
    #                     if pk_col is not None:
    #                         stmt_return = stmt.returning(pk_col)
    #                         result = conn.execute(stmt_return)
    #                         inserted_id = result.scalar()
    #                     else:
    #                         conn.execute(stmt)

    #             self.logger.info(f"✅ Core row added to '{table_or_class.name}' with ID={inserted_id}")
    #             return inserted_id

    #         else:
    #             self.logger.warning("⚠️ - Object is neither ORM class nor Core Table")
    #             return None

    #     except Exception as e:
    #         self.session.rollback()
    #         self.logger.error(f"❌ row_create failed: {e}")
    #         return None
        
    # @require_authorization
    # def row_merge(self, table_or_class: object, row_id: int, data: dict) -> bool:
    #     """
    #     Update specified columns in a row, or insert if not exists.

    #     Args:
    #         table_or_class (object): ORM class or Table object.
    #         row_id (int): ID of the row.
    #         data (dict): Column-value mapping to update.

    #     Returns:
    #         bool: True if row updated or inserted successfully.
    #     """
    #     # ✅ Gebruik expliciete None-checks (geen 'if not ...')
    #     if table_or_class is None:
    #         self.logger.warning("⚠️ - Table/class is None")
    #         return False
    #     if not data:
    #         self.logger.warning("⚠️ - Data dictionary is empty")
    #         return False

    #     try:
    #         # ✅ Bepaal ID kolom afhankelijk van type
    #         id_column = table_or_class.c.ID if hasattr(table_or_class, "c") else table_or_class.ID

    #         # 🧱 CASE 1: Core Table
    #         if hasattr(table_or_class, "c"):
    #             # Kijk of de rij al bestaat
    #             sel = select(table_or_class).where(id_column == row_id)
    #             existing = self.session.execute(sel).fetchone()

    #             if existing:
    #                 # Update
    #                 stmt = (
    #                     table_or_class.update()
    #                     .where(id_column == row_id)
    #                     .values(**data)
    #                 )
    #                 self.session.execute(stmt)
    #                 self.logger.info(f"✅ - Row ID={row_id} updated successfully")
    #             else:
    #                 # Insert nieuwe rij
    #                 new_data = {"ID": row_id, **data}
    #                 stmt = insert(table_or_class).values(**new_data)
    #                 self.session.execute(stmt)
    #                 self.logger.info(f"✅ - Row ID={row_id} inserted successfully")

    #         # 🧱 CASE 2: ORM class
    #         else:
    #             row = self.session.query(table_or_class).filter(id_column == row_id).first()
    #             if row:
    #                 for key, value in data.items():
    #                     if hasattr(row, key):
    #                         setattr(row, key, value)
    #                 self.logger.info(f"✅ - ORM Row ID={row_id} updated successfully")
    #             else:
    #                 new_data = {"ID": row_id, **data}
    #                 new_row = table_or_class(**new_data)
    #                 self.session.add(new_row)
    #                 self.logger.info(f"✅ - ORM Row ID={row_id} inserted successfully")

    #         self.session.commit()
    #         return True

    #     except Exception as e:
    #         self.session.rollback()
    #         self.logger.error(f"❌ row_merge failed: {e}")
    #         return False
    
    # @require_authorization
    # def row_replace(self, table_or_class: object, id_value: int, new_data: dict) -> bool:
    #     """
    #     Fully replace a row (overwrite existing content) for either a Core Table or ORM class.

    #     Args:
    #         table_or_class (object): SQLAlchemy Core Table or ORM class.
    #         id_value (int): The ID of the row to replace.
    #         new_data (dict): New values to write to the row.

    #     Returns:
    #         bool: True if successful, False otherwise.
    #     """
    #     if table_or_class is None:
    #         self.logger.warning("⚠️ - Table/class is None")
    #         return False
    #     if not new_data:
    #         self.logger.warning("⚠️ - new_data is empty")
    #         return False

    #     try:
    #         # ✅ Detect whether this is a Core Table or ORM class
    #         is_core = hasattr(table_or_class, "c")

    #         # --- CORE TABLE HANDLING ---
    #         if is_core:
    #             # Detect ID column name case-insensitively
    #             id_col = None
    #             for possible in ("ID", "id", "Id"):
    #                 if possible in table_or_class.c:
    #                     id_col = table_or_class.c[possible]
    #                     break
    #             if id_col is None:
    #                 raise KeyError("❌ No ID column found in Core Table")

    #             # First check if the row exists
    #             sel = select(table_or_class).where(id_col == id_value)
    #             existing = self.session.execute(sel).fetchone()

    #             if not existing:
    #                 self.logger.warning(f"⚠️ - No existing row found for ID={id_value}, inserting new one instead")
    #             else:
    #                 # Delete old row
    #                 del_stmt = delete(table_or_class).where(id_col == id_value)
    #                 self.session.execute(del_stmt)

    #             # Ensure ID is included in insert data
    #             if "ID" not in new_data and "id" not in new_data and "Id" not in new_data:
    #                 new_data["ID"] = id_value

    #             ins_stmt = insert(table_or_class).values(**new_data)
    #             self.session.execute(ins_stmt)
    #             self.session.commit()

    #             self.logger.info(f"✅ - Core row replaced successfully (ID={id_value})")
    #             return True

    #         # --- ORM HANDLING ---
    #         else:
    #             # Detect ID column
    #             id_column = getattr(table_or_class, "ID", getattr(table_or_class, "id", None))
    #             if id_column is None:
    #                 raise KeyError("❌ ORM class missing ID column")

    #             # Try to fetch existing row
    #             row = self.session.query(table_or_class).filter(id_column == id_value).first()
    #             if row:
    #                 # Replace all attributes
    #                 for key, value in new_data.items():
    #                     if hasattr(row, key):
    #                         setattr(row, key, value)
    #                 self.logger.info(f"✅ - ORM Row ID={id_value} replaced successfully")
    #             else:
    #                 # Create new one if not found
    #                 new_data = {"ID": id_value, **new_data}
    #                 new_row = table_or_class(**new_data)
    #                 self.session.add(new_row)
    #                 self.logger.info(f"✅ - ORM Row ID={id_value} inserted successfully")

    #             self.session.commit()
    #             return True

    #     except Exception as e:
    #         self.session.rollback()
    #         self.logger.error(f"❌ row_replace failed: {type(e).__name__}: {e}")
    #         return False
    
    # @ require_authorization
    # def load_row_existing_table(self, table_name: str):
    #     """
    #     Reflect an existing table from the database and return an ORM class.
    #     """
    #     Base = declarative_base()
    #     metadata = MetaData()

    #     # Reflect the table definition directly from the database
    #     table = Table(table_name, metadata, autoload_with=self.engine)

    #     # Dynamically create an ORM class bound to the reflected table
    #     orm_class = type(
    #         table_name.capitalize(),  # Class name
    #         (Base,),
    #         {"__table__": table},
    #     )

    #     return orm_class
    
    # @require_authorization
    # def load_row_existing_table(self, table_name: str):
    #     """
    #     Dynamically create an ORM class mapped to an existing table in the database.

    #     Args:
    #         table_name (str): The name of the existing table.

    #     Returns:
    #         ORM class mapped to the existing table.
    #     """
    #     with self.engine.connect() as conn:
    #         stmt = text(f"SELECT * FROM [{table_name}] WHERE 1=0")
    #         result = conn.execute(stmt)
    #         column_names = result.keys()

    #     # Build SQLAlchemy Columns dynamically
    #     columns = {}
    #     for name in column_names:
    #         if name.lower() == "id":
    #             columns[name] = Column(Integer, primary_key=True, autoincrement=True)
    #         else:
    #             columns[name] = Column(String)  # default to String

    #     # Dynamically create an ORM class
    #     orm_class = type(
    #         table_name.capitalize(),  # Class name
    #         (self.metadata,),
    #         {
    #             "__tablename__": table_name,
    #             "__table_args__": {"autoload_with": self.engine},
    #             **columns,
    #         },
    #     )

    #     return orm_class


    # def create_dynamic_table(table_name: str, columns: dict, metadata=None):
    #     """
    #     Create a dynamic table object using SQLAlchemy Core.

    #     :param table_name: Name of the table
    #     :param columns: Dictionary of column_name: column_type
    #                     e.g., {"name": String, "year": Integer}
    #     :param metadata: Optional SQLAlchemy MetaData object
    #     :return: SQLAlchemy Table object
    #     """
    #     if metadata is None:
    #         metadata = MetaData()

    #     # Always add an auto-incrementing 'id' column if not present
    #     if "id" not in columns:
    #         columns = {"id": Integer} | columns  # Python 3.9+ syntax

    #     # Build list of Column objects
    #     column_objs = []
    #     for col_name, col_type in columns.items():
    #         if col_name == "id":
    #             column_objs.append(Column(col_name, col_type, primary_key=True, autoincrement=True))
    #         else:
    #             column_objs.append(Column(col_name, col_type))

    #     # Create Table object dynamically
    #     table = Table(table_name, metadata, *column_objs)
    #     return table







    