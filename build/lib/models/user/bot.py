
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Table, Column
from sqlalchemy import Integer, Numeric, String, Boolean, Float


BOTBASE = declarative_base()
STRATEGY1BASE = declarative_base()

class InfoTable(BOTBASE):
    __tablename__ = "InfoTable"
    id =            Column(Integer,     primary_key=True, autoincrement=True)
    code =          Column(String)
    state =          Column(String)
    strategy_number =          Column(Integer)

    def __init__(self, code, state, strategy_number):
        self.code = code
        self.state = state
        self.strategy_number = strategy_number

    def __repr__(self):
        return f"{self.id}, {self.code}, {self.state}, {self.strategy_number}"
    
class KlineTable(BOTBASE):
    __tablename__ = "KlineTable"
    id =            Column(Integer,     primary_key=True, autoincrement=True)
    time =          Column(String)
    open =          Column(Float)
    high =          Column(Float)
    low =           Column(Float)
    close =         Column(Float)
    volume =        Column(Float)

    def __init__(self, time, open, high, low, close, volume):
        self.time = time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __repr__(self):
        return f"{self.id}, {self.time}, {self.open}, {self.high}, {self.low}, {self.close}, {self.volume}"

class BotTable(BOTBASE):
    __tablename__ = "BotTable"
    id =            Column(Integer,     primary_key=True)
    state =          Column(String)

    def __init__(self, state):
        self.state = state

    def __repr__(self):
        return f"{self.id}, {self.state}"

class Strategy1Table(STRATEGY1BASE):
    __tablename__ = "StrategyTable"
    id =                Column(Integer,     primary_key=True)
    RSI_short =         Column(Float)
    RSI_long =          Column(Float)
    RSI_divergence =    Column(Float)
    state =             Column(String)

    def __init__(self, RSI_short, RSI_long, RSI_divergence, state):
        self.RSI_short = RSI_short
        self.RSI_long = RSI_long
        self.RSI_divergence = RSI_divergence
        self.state = state

    def __repr__(self):
        return f"{self.id}, {self.RSI_short}, {self.RSI_long}, {self.RSI_divergence}, {self.state}"


      
        
if (__name__ == '__main__'): 
    pass