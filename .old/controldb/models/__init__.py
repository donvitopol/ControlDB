#!/usr/bin/python3.11
# print("init database")

from .user.user              import USERBASE, InfoTable, BotTable, PairTable, CoinTable, Strategy1Table
from .user.broker            import BITVAVOBASE, PairTable, AssetsTable
from .                       import user