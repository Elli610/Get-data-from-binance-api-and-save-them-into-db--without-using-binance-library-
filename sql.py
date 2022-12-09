import sqlite3

def generateDatabase():
    """
    Create a database data if not exists
    """
    conn = sqlite3.connect('apiData.db') 
    conn.close()

def deleteTable(tableName):
    """
    delete table from database apiData
    """
    conn = sqlite3.connect('apiData.db') 
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + tableName)
    conn.commit()
    conn.close()

def generateTable(pair, duration='5m'):
    """
    create table data_candles if not exists
    """
    name = pair + duration
    conn = sqlite3.connect('apiData.db') 
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS " + name + " (id INTEGER PRIMARY KEY, date INT,high REAL,low REAL,open REAL, close REAL,volume REAL)") 
    conn.commit()
    conn.close()


def generateTableNonAgregatedData(pair):
    """
    create table data_candles if not exists
    """
    name = pair + "nonAgregated"
    conn = sqlite3.connect('apiData.db') 
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS " + name + " (id INTEGER PRIMARY KEY, quantity REAL, pair TEXT, price REAL, createdAt INT, side TEXT)") 
    conn.commit()
    conn.close()

#generateDatabase()    
#generateTable("test", duration='5m')

def testQuery():
    conn = sqlite3.connect('apiData.db') 
    cursor = conn.cursor()
    query = """SELECT name FROM sqlite_master  WHERE type='table';"""
    cursor.execute(query) 
    print(cursor.fetchall()) # list of tables in db

    query = """SELECT * from BTCUSDTnonAgregated;"""
    cursor.execute(query)
    print("BTCUSDTnonAgregated :", cursor.fetchall()) # displays all data in table BTCUSDTnonAgregated

    query = """SELECT * from BTCUSDT5m;"""
    cursor.execute(query)
    print("BTCUSDT5m :", cursor.fetchall()) # displays all data in table BTCUSDT5m
    conn.close()

testQuery()