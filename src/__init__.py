#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""
ControlDB package initialization.

Exposes:
- ControlDB: Core database control class
- construct_folder_path: Utility for building folder paths
- ControlDBManager: Manages multiple ControlDB instances
- ExcelManager: Manages Excel data integration
"""
# print(" - __Init__: ControlDB ")
from .excel_manager import ExcelManager
from .utils import UtilsTable, UtilsRow, require_authorization

from .controldb import ControlDB, construct_folder_path
from .controldb_manager import ControlDBManager

from .models.root import ROOTBASE, UserTable

__all__ = [
    "ControlDB",
    "ControlDBManager",
    "ExcelManager",
    "construct_folder_path",
    "UtilsTable",
    "UtilsRow",
    "require_authorization",
]