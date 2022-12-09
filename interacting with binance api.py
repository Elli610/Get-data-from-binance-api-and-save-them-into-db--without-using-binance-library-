import time
import json
import hmac
import hashlib
import requests
from urllib.parse import urljoin, urlencode
import sql
import sqlite3

# connect to binance api
api_key = ''
api_secret  = ''

BASE_URL = 'https://api.binance.com'

dbName = "apiData"

head = {
    'X-MBX-APIKEY': api_key
}

def callApi(PATH, params=None):
    """
    Call binance api and returns the response in json format. if an error occurs, it returns None.
    """
    timestamp = int(time.time() * 1000)
    url = urljoin(BASE_URL, PATH)
    r = requests.get(url, params=params)
    if r.status_code == 200:
        # print(json.dumps(r.json(), indent=2))
        return r.json()
    else:
        print('Error: ', r.status_code, r.text)
        return None


# PATH =  '/api/v1/time'
# params = None
# print(callApi(PATH, params))

def getAllCrypto():
    """
    returns all the tradable crypto, None if an error occurs.
    """
    PATH = '/api/v1/exchangeInfo'
    params = None
    data = callApi(PATH, params)
    #print(data)
    if data:
        print('All crypto: ')
        for d in data['symbols']:
            print(d['symbol'])
        return data['symbols']
    else:
        return None


def getDepth(direction='ask', pair='BTCUSDT'):
    """
    displays and return the 'ask' or 'bid' depth of the given pair.
    """
    PATH = '/api/v3/depth'
    params = {
        'symbol': pair,
        'limit': 5000
    }
    data = callApi(PATH, params)
    #print('data: ', data)
    if data:
        if direction == 'ask':
            #print(data)
            print('Ask:')
            print(data['asks'][0][0])
            return data['asks'][0][0]
        elif direction == 'bid':
            print('Bids:')
            print(data['bids'][0][0])
            return data['bids'][0][0]
        else:
            print('Error: direction must be "ask" or "bid"')
            return None
    else:
        return None


def getOrderBook(pair):
    """
    returns the order book of the given pair.
    """
    PATH = '/api/v3/depth'
    params = {
        'symbol': pair,
        'limit': 5000
    }
    data = callApi(PATH, params)
    if data:
        return data
    else:
        return None

def refreshDataCandle(pair='BTCUSDT', duration='5m'):
    """
    returns agregated data for the given pair from binance api. None if an error occured.
    """
    PATH = '/api/v3/klines'
    params = {
        'symbol': pair,
        'interval': duration,
        'limit': 5000
    }
    data = callApi(PATH, params)
    if data:
        return data
    else:
        return None

def storeCandleModify(pair='BTCUSDT', duration='5m'):
    """
    ask for the lastest agregated data and add the last ones to db
    save the data in the 'dbName' database 
    """
    tableName = pair + duration
    conn = sqlite3.connect('apiData.db') 
    cursor = conn.cursor()
    # create table if not exists
    sql.generateTable(pair, duration='5m')
    # get last Timlestamp 
    query = "SELECT MAX(date) FROM " + tableName 
    cursor.execute(query) 
    lastDate = cursor.fetchone()[0]
    print(type(lastDate))
    if(lastDate == None):
        params = {
        'symbol': pair,
        'interval': duration,
        'limit': 5000,
        }
    else:
        params = {
        'symbol': pair,
        'interval': duration,
        'limit': 5000,
        'startTime': int(lastDate) + 1
        }
    # get the last data
    PATH = '/api/v3/klines'
    
    data = callApi(PATH, params)

    # get the last id
    query = "SELECT MAX(id) FROM " + tableName 
    cursor.execute(query) 
    lastId = cursor.fetchone()[0]
    if(lastId == None):
        id = 0
    else:
        id = int(lastId) + 1

    #add data to table
    for i in data : 
        query = ("INSERT INTO " + tableName + " (id, date, high, low, open, close, volume) VALUES( ?, ?, ?, ?, ?, ?, ?)", (id,int(i[0]), float(i[1]), float(i[2]), float(i[3]), float(i[4]), float(i[5])))
        cursor.execute(*query)
        conn.commit()
        id += 1
    #close connection to db
    conn.close()
    
# def refreshData(pair='BTCUSDT'):
#     """
#     Extract non agregated data about 'pair' from binance api (doesn't save data in db)
#     """
#     # get the last data
#     PATH = '/api/v3/trades'
#     params = {
#         'symbol': pair,
#         'limit': 1000
#     }
#     data = callApi(PATH, params)
#     return data


def refreshData(pair='BTCUSDT'):
    """
    Extract non agregated data about 'pair' from binance api (save data in db)
    """
    tableName = pair + "nonAgregated"
    conn = sqlite3.connect('apiData.db') 
    cursor = conn.cursor()
    # create table if not exists
    sql.generateTableNonAgregatedData(pair)
    # get last Timlestamp
    query = "SELECT MAX(id) FROM " + tableName
    cursor.execute(query)
    lastId = cursor.fetchone()[0]
    # get the last data
    PATH = '/api/v3/trades'
    params = {
        'symbol': pair,
        'limit': 1000
    }
    data = callApi(PATH, params)
    
    # add data to table
    for i in range (len(data)) : 
        if(lastId != None and data != None and int(data[i]['id'])>lastId):
             
            query = ("INSERT INTO " + tableName + " (id, quantity, pair, price, createdAt, side) VALUES( ?, ?, ?, ?, ?, ?)", (data[i]['id'], data[i]['qty'], pair, float(data[i]['price']), data[i]['time'], ("Buy" if(data[i]['isBuyerMaker']) else "Sell")))
            cursor.execute(*query)
            conn.commit()
    conn.close()


def createOrder(api_key, api_secret, direction, price, amount, pair='BTCUSDT_d', orderType='LimitOrder'): #not tested yet
    """
    create an order on binance with the given parameters.
    """
    PATH = '/api/v3/order'
    timestamp = int(time.time() * 1000)
    params = {
        'symbol': pair,
        'side': direction,
        'type': orderType,
        'timeInForce': 'GTC',
        'quantity': price*amount,
        'price': price,
        'recvWindow': 5000,
        'timestamp': timestamp
    }

    query_string = urlencode(params)
    params['signature'] = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    url = urljoin(BASE_URL, PATH)
    r = requests.post(url, headers=head, params=params)
    if r.status_code == 200:
        data = r.json()
        print(json.dumps(data, indent=2))
    else:
        print("Error : ", r.status_code)

def cancelOrder(api_key, api_secret, orderId): #not tested yet
    """
    cancel an order on binance with the given parameters.
    """
    PATH = '/api/v3/order'
    timestamp = int(time.time() * 1000)
    params = {
        'orderId': orderId,
        'recvWindow': 5000,
        'timestamp': timestamp
    }

    query_string = urlencode(params)
    params['signature'] = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    url = urljoin(BASE_URL, PATH)
    r = requests.delete(url, headers=head, params=params)
    if r.status_code == 200:
        data = r.json()
        print(json.dumps(data, indent=2))
    else:
        print("Error : ", r.status_code)




#a = getAllCrypto()
#b = getDepth()
#print(getOrderBook('BTCUSDT'))
#print(refreshDataCandle()[0])
#storeCandleModify(pair='BTCUSDT', duration='5m')
#print(refreshData())

