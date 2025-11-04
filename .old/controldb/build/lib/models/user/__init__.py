#!/usr/bin/python3.11
# print("init database")
from .user              import USERBASE, InfoTable, BotTable, BrokerTable, PairTable, CoinTable, StrategyTable, Strategy1Table
from .broker            import BITVAVOBASE, PairTable, AssetsTable
from .bot               import BOTBASE, KlineTable
from .bot               import STRATEGY1BASE, Strategy1Table
from .                  import bot
from .                  import user