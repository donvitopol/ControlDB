

import unittest
from sqlalchemy import Engine
from controldb import ControlDBGet, ControlDB
from mydb import MyDB
import os

class TestUtils(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def equal_test(self, name:str, para:any, ref:any, expl:str=None, _print=False)->None:
        if _print:
            print (f" - Test logger {name}:")
            print (f"     -> Parameter:     ", para)
            print (f"     => Refference:    ", ref)
            print ("")

        self.assertEqual(para, ref, expl) 

    def logger_test(self, name:str, para:any, ref:any, obj:object, _print=False)->None:
        if _print:
            print (f" - Test logger: {name}")
            print (f"     -> Parameter:     ", para)
            print (f"     => Refference:    ", ref)
            print ("")

        self.assertEqual(para, ref,                             f"Test logger: {name} -> Check database path: Backup directory.") 
        self.assertTrue(hasattr(obj, 'logger'),                 f"Test logger: {name} -> Test if logger is initialized.")  

    def get_test(self, db:ControlDB, _print=False):
        if _print:
            print (f" - get_test: {type(db)}")
            print (f"    - Objecte had attribute 'Get': ", hasattr(db, 'get'))
            print (f"    - Test if ControlDBGet is initized: ")
            print (f"        -> Parameter:     ", str(type(db.get)))
            print (f"        => Refference:    ", "ControlDBGet")
            print ("")

        self.assertTrue(hasattr(db, 'get'),                "Test if get has attribute.")  #
        self.assertTrue("ControlDBGet" in str(type(db.get)),      "Test if ControlDBGet is initized.") 

    def connect_true_test(self, db:MyDB, engine:Engine, _print=False):
        if _print:
            print (f" - connect_true_test: {type(db)}")
            print (f"    - Engine Type: ")
            print (f"        -> Parameter:     ", type(engine))
            print (f"        => Refference:    ", Engine)
            print (f"    - Engine connected: ", db.connected)
            print ("")

        self.assertEqual(type(engine), Engine,             "Test if engine is initized.")  
        self.assertTrue(db.connected,                      "Test connected after connection.")  #
        self.get_test(db, _print=_print)

    def connect_false_test(self, db:MyDB, engine:Engine, _print=False):
        if _print:
            print (f" - connect_false_test: {type(db)}")
            print (f"    - Engine Type: ")
            print (f"        -> Parameter:     ", type(engine))
            print (f"        => Refference:    ", type(None))
            print (f"    - Engine connected: ", db.connected)
            print ("")

        self.assertEqual(type(engine), type(None),         "Test if engine is initized.")  
        self.assertFalse(db.connected,                      "Test connected after connection.")  #
        # self.get_test(db, _print=_print)

    def path_true_test(self, name:str, ref:any, para:any, expl:str=None, _print=False):
        if _print:
            print (f" - Test Path {name}:")
            print (f"     => Refference:    ", ref)
            print (f"     -> Parameter:     ", para)
            print ("")

        self.assertEqual(para, ref, expl) 
        self.assertTrue(os.path.exists(para), expl) 
        
    def path_false_test(self, name:str, para:any, ref:any, expl:str=None, _print=False):
        if _print:
            print (f" - Test Path {name}:")
            print (f"     -> Parameter:     ", para)
            print (f"     => Refference:    ", ref)
            print ("")

        self.assertEqual(para, ref, expl) 
        self.assertFalse(os.path.exists(para), expl) 