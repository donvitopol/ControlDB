
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Table, Column
from sqlalchemy import Integer, Numeric, String, Boolean, Float


USERBASE = declarative_base()

class InfoTable(USERBASE):
    __tablename__ = "InfoTable"
    id =            Column(Integer, primary_key=True, autoincrement=True)
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
    id =                Column(Integer, primary_key=True, autoincrement=True)
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
    id =                Column(Integer, primary_key=True, autoincrement=True)
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
    id =                Column(Integer, primary_key=True, autoincrement=True)
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
    id =            Column(Integer,     primary_key=True,   autoincrement=True)
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
    id =                Column(Integer,     primary_key=True,   autoincrement=True)
    number =            Column(Integer,     nullable=False)
    strategyXID =       Column(Integer)

    def __init__(self, number):
        self.number = number
        self.strategyXID = 0

    def __repr__(self):
        return f"{self.number, self.strategyXID}"
    

class Strategy1Table(USERBASE):
    __tablename__ = "Strategy1Table"
    id =                Column(Integer,     primary_key=True,   autoincrement=True)
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
      
        
if (__name__ == '__main__'):  
    import os, sys
    sourcePath =  os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    sys.path.append(sourcePath)
    from modules.database.database_files import connect, create, Engine  
    mainPath = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", ".."))
    print(mainPath)
    dbFile = os.path.join(mainPath, "database", f"user.mdb")
    print(dbFile)

    try:
        create(dbFile)
    finally:
        engine:Engine = connect(dbFile)


    userBase.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Password to Hash
    my_password = b'maffia'

    
    ################################################################
    #######                SETUP USER TABLE                  #######
    ################################################################
    password = "maffia"
    info = InfoTable("Vito Pol", "DonVitoPol", "don_vito_pol@hotmail.com", my_password)
    session.add(info)
    session.commit()

    
    ################################################################
    #######               SETUP BROKER TABLE                 #######
    ################################################################
    broker_1 = BrokerTable(
                        "Bitvavo", "https://bitvavo.com/", 
                        '9d1f0b98cbb4e8985e7f3da7a832d37ecbdd63177cf31c7fb0e78e7b36fa38d7',
                        'f59b584ec0ad271a9e89c5c37ebdeb109cdb85ee4ef6dfa47dd66be0e5eaa21b831e5ac3c2a385ee28a63a21ca14bbfd7b422624a13e5527d6d8883972de9f37',
                        '')
    broker_2 = BrokerTable(
                        "KuCoin", "https://kucoin.com/", 
                        '650624f2eebd89000110ab31',
                        '78dd6077-fdef-4129-ab3f-106c8f7a6530',
                        'Mxbjrnb@:@HHWTcfcqJ.2ondI.0M/xaD')
    session.add(broker_1)
    session.add(broker_2)

    
    session.commit()