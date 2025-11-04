
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Table, Column
from sqlalchemy import Integer, Numeric, String, Boolean, Float


USERBASE = declarative_base()

class InfoTable(USERBASE):
    __tablename__ = "InfoTable"
    ID =                Column(Integer, primary_key=True, autoincrement=True)
    username =      Column(String,  primary_key=True, nullable=False)
    fullname =      Column(String,  nullable=False)
    email =         Column(String,  nullable=False)
    password =      Column(String)

    def __init__(self, fullname, username, email, password):
        self.fullname = fullname
        self.username = username
        self.email = email
        self.password = password

    def __repr__(self):
        return f"{self.fullname}, {self.username}, {self.email}, {self.password}"


class CoinTable(USERBASE):
    __tablename__ = "CoinTable"
    ID =                Column(Integer, primary_key=True, autoincrement=True)
    symbol =            Column(String)
    free =              Column(Float)
    active =            Column(Float)
    reserved =          Column(Float)
    total =             Column(Float)

    tradeID =           Column(Integer)
    brokerID =          Column(Integer)
    pairID =            Column(Integer)

    def __init__(self, symbol, free, active=0.00, reserved=0.00):
        self.symbol = symbol
        self.free = free
        self.active = active
        self.reserved = reserved
        self.total = free + active + reserved

    def __repr__(self):
        return f"{self.id}, {self.symbol}, {self.free}, {self.active}, {self.reserved}, {self.total}, {self.tradeID}, {self.brokerID}, {self.pairID}"


class PairTable(USERBASE):
    __tablename__ = "PairTable"
    ID =                Column(Integer, primary_key=True, autoincrement=True)
    name =              Column(String)
    baseID =            Column(Integer)
    quoteID =           Column(Integer)
    botID =           Column(Integer)
    majorSymbol =       Column(String)
    minorSymbol =       Column(String)

    def __init__(self, name, baseID:int, quoteID:int, majorSymbol:str, minorSymbol:str):
        self.name = name
        self.baseID = baseID
        self.quoteID = quoteID
        self.majorSymbol = majorSymbol
        self.minorSymbol = minorSymbol
        self.botID = 0

    def __repr__(self):
        return f"{self.id}, {self.name}, {self.baseID}, {self.quoteID}, {self.botID}, {self.majorSymbol}, {self.minorSymbol}"


class BotTable(USERBASE):
    __tablename__ = "BotTable"
    ID =                Column(Integer, primary_key=True, autoincrement=True)
    pairID =            Column(Integer)
    baseID =            Column(Integer)
    quoteID =           Column(Integer)
    brokerID =          Column(Integer)
    strategyID =        Column(Integer)
    code =              Column(String)
    inTrade =           Column(Boolean)

    def __init__(self, pairID, baseID:int, quoteID:int, brokerID:int, strategyID:int):
        self.pairID = pairID
        self.baseID = baseID
        self.quoteID = quoteID
        self.brokerID = brokerID
        self.strategyID = strategyID
        self.inTrade = False
        self.code = ""

        

    def __repr__(self):
        return f"{self.id}, {self.pairID}, {self.baseID}, {self.quoteID}, {self.brokerID}, {self.strategyID}, {self.inTrade}, {self.code}"


class BrokerTable(USERBASE):
    __tablename__ = "BrokerTable"
    ID =                Column(Integer, primary_key=True, autoincrement=True)
    name =          Column(String(12),  unique=True,  nullable=False)
    url =           Column(String,      nullable=False)
    # name =          Column(String(12),  unique=True,        nullable=False)
    # url =           Column(String,      unique=True,        nullable=False)
    key =           Column(String)
    secret =        Column(String)
    passphrase =    Column(String)

    def __init__(self, name, url, key, secret, passphrase):
        self.name = name
        self.url = url
        self.key = key
        self.secret = secret
        self.passphrase = passphrase

    def __repr__(self):
        return f"{self.id}, {self.name}, {self.url}, {self.key}, {self.secret}, {self.passphrase}"
        

class StrategyTable(USERBASE):
    __tablename__ = "StrategyTable"
    ID =                Column(Integer, primary_key=True, autoincrement=True)
    number =            Column(Integer,     nullable=False)
    strategyXID =       Column(Integer)

    def __init__(self, number):
        self.number = number
        self.strategyXID = 0

    def __repr__(self):
        return f"{self.number, self.strategyXID}"
    

class Strategy1Table(USERBASE):
    __tablename__ = "Strategy1Table"
    ID =                Column(Integer, primary_key=True, autoincrement=True)
    strategyID =        Column(Integer,     nullable=False)
    number =            Column(Integer,     nullable=False)
    shortOption =       Column(Boolean,     nullable=False)
    orderType =         Column(String,      nullable=False)
    longPeriod =        Column(Integer)
    shortPeriod =       Column(Integer)
    highLevel =         Column(Float)
    lowLevel =          Column(Float)
    buyPercentage =     Column(Float)
    sellPercentage =    Column(Float)

    def __init__(self, config:dict[str, any]):
        self.strategyID = config["strategyID"]
        self.number = config["number"]

        self.shortOption = config["shortOption"]
        self.orderType = config["orderType"]

        self.shortPeriod = config["shortPeriod"]
        self.longPeriod = config["longPeriod"]

        self.lowLevel = config["lowLevel"]
        self.highLevel = config["highLevel"]

        self.lowVolume = config["lowVolume"]
        self.highVolume = config["highVolume"]

        self.buyPercentage = config["buyPercentage"]
        self.sellPercentage = config["sellPercentage"]

    def __repr__(self):
        row = f"{self.id}"
        row =+ f", {self.strategyID}, {self.number}"
        row =+ f", {self.shortOption}, {self.orderType}"
        row =+ f", {self.longPeriod}, {self.shortPeriod}"
        row =+ f", {self.highLevel}, {self.lowLevel}"
        row =+ f", {self.highVolume}, {self.lowVolume}"
        row =+ f", {self.buyPercentage}, {self.sellPercentage}"
        return row
      
