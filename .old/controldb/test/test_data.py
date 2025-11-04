
from datetime import datetime, date
from pandas import DataFrame

TESTTABLE1 = {}
TESTTABLE1['FullName'] = 'CHAR'
TESTTABLE1['UserName'] = 'FLOAT'
TESTTABLE1['Email'] = 'BIT'
TESTTABLE1['EmployeeNumber'] = 'INT'
TESTTABLE1['Screens'] = 'INT'
TESTTABLE1['Enzymes'] = 'INT'
TESTTABLE1['Hits'] = 'INT'
TESTTABLE1['Included'] = 'DATETIME'

t = datetime.now()
logData = date(t.year, t.month, t.day)
TESTRECORD1 = {}
TESTRECORD1['FullName'] = 'Test'
TESTRECORD1['UserName'] = 0.0
TESTRECORD1['Email'] = -1
TESTRECORD1['EmployeeNumber'] = 100002
TESTRECORD1['Screens'] = 0
TESTRECORD1['Enzymes'] = 5
TESTRECORD1['Hits'] = 6
TESTRECORD1['Included'] = logData


TESTDF1 = DataFrame({}, columns=TESTTABLE1.keys())
for i in range (10):
    # print(TESTTABLE1.keys())
    # print(list(TESTRECORD1.values()))
    TESTDF1.loc[i, TESTTABLE1.keys()] = list(TESTRECORD1.values())

