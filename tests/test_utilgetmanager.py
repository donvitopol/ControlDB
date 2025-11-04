import unittest
from unittest.mock import MagicMock, patch
from src.util_manager import UtilGetManager
import os
import tempfile
from src import ControlDB
from src import ROOTBASE
from tests.utils import temp_controldb, close_db, create_dummy_dataframe, make_temp_excel, temp_excel_manager, safe_remove_folder


class TestUtilManager(unittest.TestCase):
    """Unit tests for UtilGetManager class."""

    def setUp(self):
        """Create a temporary database folder and ControlDB instance."""
        self.password="secret"
        self.db_name = "test_db"  
        self.folderSystem=None
        self.db_type="mdb"

        self.temp_dir = tempfile.TemporaryDirectory()    
        self.root_path = os.path.join(self.temp_dir.name, self.db_name)
        self.file_path = os.path.join(self.root_path, f"{self.db_name}.{self.db_type}")

        self.db:ControlDB
        self.db = temp_controldb(
            self.db_name, 
            self.root_path,
            folderSystem=self.folderSystem,
            db_type= self.db_type,
            password=self.password,
            base=ROOTBASE,
            logLevel=10
        )

    def tearDown(self):
        """Clean up the temporary folder."""
        close_db(self.temp_dir, self.db)

    def test_connect_assigns_engine_and_session(self):

        pass

if __name__ == "__main__":
    unittest.main()