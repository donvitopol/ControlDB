
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Table, Column
from sqlalchemy import Integer, Numeric, String, Boolean, Float, Double


BITVAVOBASE = declarative_base()
    
class PairTable(BITVAVOBASE):
    __tablename__ = "PairTable"
    id =                    Column(Integer, primary_key=True, autoincrement=True)
    symbol =                Column(String)
    base =                  Column(String)
    quote =                 Column(String)
    price =                 Column(Double)

    def __init__(self, symbol, base, quote, price):
        self.symbol = symbol
        self.base = base
        self.quote = quote
        self.price = price

    def __repr__(self):
        return f"{self.id}, {self.symbol}, {self.base}, {self.quote}, {self.price}"
     
class AssetsTable(BITVAVOBASE):
    __tablename__ = "AssetsTable"
    id =                    Column(Integer, primary_key=True, autoincrement=True)
    symbol =                Column(String)
    available =             Column(Double)
    inOrder =               Column(Double)
    total =                 Column(Double)

    def __init__(self, symbol, available, inOrder, total):
        self.symbol = symbol
        self.available = available
        self.inOrder = inOrder
        self.total = total

    def __repr__(self):
        return f"{self.id}, {self.symbol}, {self.available}, {self.inOrder}, {self.total}"
     
class FeesTable(BITVAVOBASE):
    __tablename__ = "FeesTable"
    id =                    Column(Integer, primary_key=True, autoincrement=True)
    tier =                  Column(Integer)
    volume =                Column(Double)
    maker =                 Column(Double)
    taker =                 Column(Double)

    def __init__(self, tier, volume, maker, taker):
        self.tier = tier
        self.volume = volume
        self.maker = maker
        self.taker = taker

    def __repr__(self):
        return f"{self.id}, {self.tier}, {self.volume}, {self.maker}, {self.taker}"
  