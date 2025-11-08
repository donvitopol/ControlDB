import pandas as pd
from typing import Optional, Dict, Any

from sqlalchemy import Engine, MetaData, Table, Column, Integer, String
from sqlalchemy import insert, select, delete, text, update, Table, Column, Integer, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from pretty_logger import prettylog, PrettyLogger

from .decorators import require_authorization
from .utils_row import UtilsRow


@prettylog
class UtilsTable:
    """CRUD helper class for managing ORM and Core SQLAlchemy Tables."""
    @property
    def authorized(self) -> bool:
        """Check if the manager is authorized."""
        return getattr(self, "_authorized", True)
    
    @property
    def row(self) -> UtilsRow:
        """Database ID."""
        return self.__row

    def __init__(self,
        table_class: object = None,
        engine: Optional[Engine] = None,
        session: Optional[Session] = None,
        base: Optional[MetaData | list[MetaData]] = None,
        logLevel: int = 30
    ):
        """
        Initialize the UtilsTable manager.

        Args:
            table_class (object): SQLAlchemy Table or ORM class.
            engine (Optional[Engine]): SQLAlchemy engine to connect.
            session (Optional[Session]): SQLAlchemy session to use.
            base (Optional[MetaData | list[MetaData]]): Optional metadata/base.
            logLevel (int): Logging level (default: 30).
        """
        self.table_class: Table | object = table_class
        self.logLevel = logLevel

        self.logger: PrettyLogger
        self.row_id: int = None
        
        self._authorized = False
        self.engine: Optional[Engine]
        self.session: Optional[Session]
        self.base: Optional[MetaData | list[MetaData]] = base

        # Initialize UtilsRow (temporary, will reconnect if needed)
        self.__row: UtilsRow = UtilsRow(self.table_class, logLevel=logLevel)

        # Auto-connect if engine is provided
        if engine is not None:
            if session is None:
                SessionLocal = sessionmaker(bind=engine)
                session = SessionLocal()
            self.connect(
                table_class=table_class,
                engine=engine,
                session=session,
                base=base,
            )
            
    # ---------------------- 🔹 CONNECTION ----------------------

    def _detect_kind_and_name(self, table_class: object):
        """Return a tuple (is_core, kind_str, name) for logging purposes."""
        is_core = hasattr(table_class, "c")
        kind = "Core Table" if is_core else "ORM Class"
        name = getattr(table_class, "__tablename__", getattr(table_class, "name", "Unknown"))
        return is_core, kind, name
    
    def connect(
        self,
        table_class: object,
        engine: Engine,
        session: Session,
        base: MetaData | list[MetaData],
    ):
        """
        Connect the manager (and UtilsRow) to a specific SQLAlchemy table, engine, and session.

        Args:
            table_class (object): SQLAlchemy Table or ORM class (required).
            engine (Engine): SQLAlchemy engine.
            session (Session): SQLAlchemy session.
            base (MetaData | list[MetaData]): SQLAlchemy MetaData.
        """
        # Update internal references
        self.table_class = table_class
        self.engine = engine
        self.session = session
        self.base = base

        # Detect Core vs ORM
        self.is_core, kind, name = self._detect_kind_and_name(self.table_class)

        # Reconnect UtilsRow
        self.__row.connect(engine, session, base)

        self._authorized = True
        self.logger.debug(f"✅ UtilsTable connected for {kind}: {name}")
    
    # ---------------------- 🔹 COLUMN INFO ----------------------

    def get_column_names(self) -> list[str]:
        """Return all column names."""
        if self.is_core:
            return [c.name for c in self.table_class.columns]
        return [c.name for c in self.table_class.__table__.columns]
    
    @require_authorization
    def get_column_names(self) -> list[str]:
        """
        Return the list of column names from the managed SQLAlchemy table.

        Uses self.is_core to distinguish between Core Table and ORM Class.

        Returns:
            list[str]: List of column names.

        Raises:
            ValueError: If the table_class is invalid or not connected.
        """
        if self.table_class is None:
            raise ValueError("⛔ - table_class is not set in UtilsTable instance.")

        # Core Table (from metadata)
        if self.is_core:
            return [col.name for col in self.table_class.columns]

        # ORM Class (uses __table__)
        if hasattr(self.table_class, "__table__"):
            return [col.name for col in self.table_class.__table__.columns]

        raise ValueError("⛔ - Invalid table_class: not Core or ORM type.")

    def get_column_definitions(self) -> dict[str, type]:
        """Return mapping of column_name → SQLAlchemy type."""
        cols = self.table_class.columns if self.is_core else self.table_class.__table__.columns
        return {c.name: type(c.type) for c in cols}

    def get_column_as_list(self, column_name: str) -> list:
        """Return all values from a given column."""
        if self.is_core:
            column = self.table_class.columns.get(column_name)
            if not column:
                raise ValueError(f"Column '{column_name}' not found in {self.table_class.name}")
            with self.engine.connect() as conn:
                result = conn.execute(select(column))
                return [row[0] for row in result]
        else:
            column = getattr(self.table_class, column_name, None)
            if not column:
                raise ValueError(f"Column '{column_name}' not found in {self.table_class.__tablename__}")
            return [row[0] for row in self.session.query(column).all()]

    # ---------------------- 🔹 IDS & ROWS ----------------------

    def get_first_free_id(self, pk: str = "ID") -> int:
        """Find the first available (gapless) ID value."""
        try:
            if self.is_core:
                col = self.table_class.c[pk]
                ids = [r[0] for r in self.session.execute(select(col)).all()]
            else:
                col = getattr(self.table_class, pk)
                ids = [r[0] for r in self.session.query(col).all()]

            ids = sorted([i for i in ids if i is not None])
            if not ids:
                return 1

            for expected, actual in enumerate(ids, start=1):
                if expected != actual:
                    return expected
            return ids[-1] + 1
        except Exception as e:
            self.logger.error(f"Error finding first free ID: {e}")
            return -1

    def get_row_dict(self, id: int) -> dict | None:
        """Fetch a row by ID as a dict."""
        filter_col = self.table_class.c.ID if self.is_core else self.table_class.ID
        result = (
            self.session.execute(self.table_class.select().where(filter_col == id)).first()
            if self.is_core
            else self.session.query(self.table_class).filter(filter_col == id).first()
        )

        if not result:
            return None

        if hasattr(result, "_mapping"):  # SQLAlchemy Row
            return dict(result._mapping)
        if hasattr(result, "__table__"):  # ORM Instance
            return {col.name: getattr(result, col.name) for col in result.__table__.columns}
        return {k: v for k, v in vars(result).items() if not k.startswith("_")}

    # ---------------------- 🔹 TABLE OPERATIONS ----------------------

    def get_df_table(self) -> pd.DataFrame | None:
        """Return the full table as a pandas DataFrame."""
        table_name = getattr(self.table_class, "__tablename__", getattr(self.table_class, "name", None))
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(f"SELECT * FROM [{table_name}]", conn)
            if "ID" in df.columns:
                df = df.set_index("ID")
            return df
        except Exception as e:
            self.logger.warning(f"⚠️ - Get Table failed: {e}")
            return None

    @require_authorization
    def update_column_value(self, row_id: int, column_name: str, new_value):
        """Update a single column value for a given row."""
        table_name = getattr(self.table_class, "__tablename__", getattr(self.table_class, "name", None))
        if self.is_core:
            stmt = (
                self.table_class.update()
                .where(self.table_class.c.ID == row_id)
                .values({column_name: new_value})
            )
            self.session.execute(stmt)
        else:
            row = self.session.query(self.table_class).filter(self.table_class.ID == row_id).first()
            if not row:
                raise ValueError(f"Row with ID={row_id} not found in {table_name}")
            setattr(row, column_name, new_value)
        self.session.commit()
        self.logger.debug(f"✅ - Updated row {row_id}: {column_name} = {new_value}")
        return True

    @require_authorization
    def add_columns(self, columns: dict):
        """Add multiple columns to a table."""
        table_name = getattr(self.table_class, "__tablename__", getattr(self.table_class, "name", None))
        try:
            for col_name, col_type in columns.items():
                type_name = col_type().compile(dialect=self.engine.dialect)
                sql = f'ALTER TABLE {table_name} ADD COLUMN {col_name} {type_name}'
                self.session.execute(sql)
                self.logger.debug(f"➕ Added column '{col_name}' ({type_name})")
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            self.logger.warning(f"⚠️ - Failed to add columns to {table_name}: {e}")
            raise
