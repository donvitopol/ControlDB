import pandas as pd
import functools
from sqlalchemy import Engine, MetaData, Table, Column, Integer, String
from sqlalchemy import insert, delete, select, text, update
from sqlalchemy.orm import sessionmaker, Session, DeclarativeMeta
from pretty_logger import PrettyLogger, prettylog
from sqlalchemy import update

from src import UtilsRow



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







    