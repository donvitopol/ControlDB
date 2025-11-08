# UtilsRow.py

import pandas as pd
from typing import Optional, Dict, Any

from sqlalchemy import Engine, MetaData, Table, Column, Integer, String
from sqlalchemy import insert, select, delete, text, update, Table, Column, Integer, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from pretty_logger import prettylog, PrettyLogger

from .decorators import require_authorization

@prettylog
class UtilsRow:
    """CRUD helper class for managing rows in ORM classes and Core Tables."""

    @property
    def authorized(self) -> bool:
        """Check if the manager is authorized."""
        return getattr(self, "_authorized", True)

    @property
    def id(self) -> int:
        """ID."""
        return self.__id

    @id.setter
    def id(self, value: int) -> None:
        """Set ID."""
        self.__id = value

    def __init__(self, table_class: object, row_id: Optional[int] = None, logLevel: int = 30):
        """
        Initialize the UtilsRow manager.

        Args:
            table_class (object): SQLAlchemy Core Table or ORM class.
            row_id (Optional[int]): Optional row ID for default operations.
            logLevel (int): Logging level (default: 30).
        """
        self.logger: PrettyLogger
        self.engine: Optional[Engine] = None
        self.session: Optional[Session] = None
        self.base: Optional[MetaData | list[MetaData]] = None
        self.table_class = table_class
        self.row_id = row_id
        self.logLevel = logLevel
        self._authorized = True

        # Detect whether table_class is Core Table or ORM
        self.is_core = hasattr(self.table_class, "c")
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
        
        # Detect whether table_class is Core Table or ORM
        self.logger.debug(f"UtilsRow initialized with {'Core Table' if self.is_core else 'ORM class'}")

        self.logger.debug(" -> UtilsRow connected")

    def _get_id_column(self) -> object:
        """
        Detect the ID column in the stored table_class.

        Returns:
            Column | attribute: ID column or attribute.
        """
        if not hasattr(self, "table_class") or self.table_class is None:
            raise ValueError("⛔ - table_class is not set")
        
        id_column = None
        if self.is_core:  # Core Table
            for c in ("ID", "id", "Id"):
                if c in self.table_class.c:
                    id_column = self.table_class.c[c]
                    break
        else:  # ORM
            for c in ("ID", "id", "Id"):
                if hasattr(self.table_class, c):
                    id_column = getattr(self.table_class, c)
                    break

        if id_column is None:
            raise KeyError("❌ No ID column found")

        return id_column

    @require_authorization
    def get(self, id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row by id (optional) from stored table_class.
        If id is provided, update self.row_id automatically.

        Args:
            id (Optional[int]): Optional ID of the row to fetch.

        Returns:
            Optional[Dict[str, Any]]: Row data as dict, or None if not found.
        """
        if id is not None:
            self.row_id = id

        if self.row_id is None:
            self.logger.warning("⚠️ - No row_id provided for get")
            return None

        id_column = self._get_id_column()
        if self.is_core:
            result = self.session.execute(select(self.table_class).where(id_column == self.row_id)).first()
        else:
            result = self.session.query(self.table_class).filter(id_column == self.row_id).first()

        if not result:
            return None
        if self.is_core:  # Core Table Row
            return dict(result._mapping)
        if hasattr(result, "__table__"):  # ORM instance
            return {col.name: getattr(result, col.name) for col in result.__table__.columns}

        return {k: v for k, v in vars(result).items() if not k.startswith("_")}

    @require_authorization
    def set_id(self, row_id: int):
        """
        Set the internal row_id to target an existing row.

        Args:
            row_id (int): The ID of the row to target.
        """
        if row_id is None:
            raise ValueError("⛔ - row_id cannot be None")
        self.row_id = row_id
        self.logger.debug(f"🟢 - row_id set to {row_id}")

    @require_authorization
    def create(self, *args, **kwargs) -> int | None:
        """
        Insert a new row into the stored table_class.

        Returns:
            int | None: The inserted row ID if available, else None.
        """
        if self.table_class is None:
            self.logger.warning("⚠️ - Table/class not provided")
            return None
        if self.engine is None or self.session is None:
            raise RuntimeError("⛔ - Manager is not connected")

        try:
            if not self.is_core:
                row = self.table_class(*args, **kwargs)
                self.session.add(row)
                self.session.commit()
                self.row_id = getattr(row, "ID", getattr(row, "id", getattr(row, "Id", None)))
                return self.row_id
            else:
                stmt = insert(self.table_class).values(**kwargs)
                inserted_id = None
                with self.engine.begin() as conn:
                    if "access" in str(self.engine.url).lower():
                        conn.execute(stmt)
                        result = conn.execute(text("SELECT @@IDENTITY AS last_id"))
                        inserted_id = result.scalar()
                    else:
                        pk_col = next((c for c in self.table_class.columns if c.primary_key), None)
                        if pk_col is not None:
                            stmt_return = stmt.returning(pk_col)
                            result = conn.execute(stmt_return)
                            inserted_id = result.scalar()
                        else:
                            conn.execute(stmt)

                self.row_id = inserted_id
                self.logger.info(f"✅ Core row added to '{self.table_class.name}' with ID={inserted_id}")
                return inserted_id

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"❌ row_create failed: {e}")
            return None

    @require_authorization
    def merge(self, data: Dict[str, Any]) -> bool:
        """
        Update specified columns in a row, or insert if not exists.
        If row_id is not set, a new row will be created first.

        Args:
            data (Dict[str, Any]): Column-value mapping to update.

        Returns:
            bool: True if row updated or inserted successfully.
        """
        if self.row_id is None:
            self.logger.debug("ℹ️ - row_id not set, creating new row first")
            new_id = self.create(**data)
            return new_id is not None

        try:
            id_column = self._get_id_column()
            if self.is_core:
                sel = select(self.table_class).where(id_column == self.row_id)
                existing = self.session.execute(sel).fetchone()
                if existing:
                    stmt = self.table_class.update().where(id_column == self.row_id).values(**data)
                    self.session.execute(stmt)
                    self.logger.info(f"✅ - Row ID={self.row_id} updated successfully")
                else:
                    new_data = {"ID": self.row_id, **data}
                    self.session.execute(insert(self.table_class).values(**new_data))
                    self.logger.info(f"✅ - Row ID={self.row_id} inserted successfully")
            else:
                row = self.session.query(self.table_class).filter(id_column == self.row_id).first()
                if row:
                    for k, v in data.items():
                        if hasattr(row, k):
                            setattr(row, k, v)
                    self.logger.info(f"✅ - ORM Row ID={self.row_id} updated successfully")
                else:
                    new_data = {"ID": self.row_id, **data}
                    self.session.add(self.table_class(**new_data))
                    self.logger.info(f"✅ - ORM Row ID={self.row_id} inserted successfully")
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"❌ row_merge failed: {e}")
            return False

    @require_authorization
    def replace(self, new_data: Dict[str, Any]) -> bool:
        """
        Fully replace a row (overwrite existing content) for Core Table or ORM class.
        If row_id is not set, a new row will be created first.

        Args:
            new_data (Dict[str, Any]): New values to write to the row.

        Returns:
            bool: True if successful, False otherwise.
        """
        if self.row_id is None:
            self.logger.debug("ℹ️ - row_id not set, creating new row first")
            new_id = self.create(**new_data)
            return new_id is not None

        try:
            id_column = self._get_id_column()
            if self.is_core:
                sel = select(self.table_class).where(id_column == self.row_id)
                existing = self.session.execute(sel).fetchone()
                if existing:
                    self.session.execute(delete(self.table_class).where(id_column == self.row_id))
                if "ID" not in new_data and "id" not in new_data and "Id" not in new_data:
                    new_data["ID"] = self.row_id
                self.session.execute(insert(self.table_class).values(**new_data))
            else:
                row = self.session.query(self.table_class).filter(id_column == self.row_id).first()
                if row:
                    for k, v in new_data.items():
                        if hasattr(row, k):
                            setattr(row, k, v)
                else:
                    new_data["ID"] = self.row_id
                    self.session.add(self.table_class(**new_data))

            self.session.commit()
            self.logger.info(f"✅ Row ID={self.row_id} replaced successfully")
            return True
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"❌ row_replace failed: {type(e).__name__}: {e}")
            return False

    @require_authorization
    def delete(self) -> bool:
        """
        Delete the stored row by ID.

        Returns:
            bool: True if deleted successfully, False otherwise.
        """
        if self.row_id is None:
            self.logger.warning("⚠️ - No row_id provided for delete")
            return False

        try:
            id_column = self._get_id_column()
            if self.is_core:
                self.session.execute(delete(self.table_class).where(id_column == self.row_id))
            else:
                row = self.session.query(self.table_class).filter(id_column == self.row_id).first()
                if row:
                    self.session.delete(row)
            self.session.commit()
            self.logger.info(f"✅ Row ID={self.row_id} deleted successfully")
            self.row_id = None
            return True
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"❌ row_delete failed: {type(e).__name__}: {e}")
            return False
