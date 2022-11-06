import ccxt
import datetime
from threading import Thread
import pandas as pd
from const import coins_all, coins_bybit, coins
import numpy as np
import json
from postgresql_cross import con, cur

start_time = datetime.datetime.now()
print(start_time)
'''BYBIT DATA'''
def bybit():
    bybit = ccxt.bybit()
    bybit_list = {}
    bybit_list_symbols = []
    global list_bybit
    list_bybit = []
    for i in coins_bybit:
        try:
            res = bybit.fetch_ticker(i)
            bybit_list[f'{i}'] = res
            dict_tmp = res['symbol']
            bybit_list_symbols.append(dict_tmp)
        except:
            pass

    for i in coins_bybit:
        if i in bybit_list_symbols:
            symbol = bybit_list[f'{i}']['symbol']
            price = bybit_list[f'{i}']['last']
            dict_tmp_bybit = price
            list_bybit.append(dict_tmp_bybit)
        else:
            symbol = i
            price = np.nan
            dict_tmp_bybit = price
            list_bybit.append(dict_tmp_bybit)
    # print('BYBIT: ', list_bybit)
    return list_bybit

'''BINANCE DATA'''
def binance():
    global list_binance
    binance = ccxt.binance()
    # print(binance.fetch_tickers())
    binance_list = {}
    binance_list_symbols = []
    list_binance = []

    for i in coins_all:
        try:
            res = binance.fetch_ticker(i)
            binance_list[f'{i}'] = res
            binance_tmp = res['symbol']
            binance_list_symbols.append(binance_tmp)
        except:
            pass

    for i in coins_all:
        if i in binance_list_symbols:
            symbol = binance_list[f'{i}']['symbol']
            price = binance_list[f'{i}']['last']
            binance_tmp_ = price
            list_binance.append(binance_tmp_)
        else:
            symbol = i
            price = np.nan
            binance_tmp_ = price
            list_binance.append(binance_tmp_)
    # print('Binance: ', list_binance)
    return list_binance



'''ftx'''
def ftx():
    global list_ftx
    ftx = ccxt.ftx()
    # print(ftx.fetch_tickers())
    ftx_list = {}
    ftx_list_symbols = []
    list_ftx = []

    for i in coins_all:
        try:
            res = ftx.fetch_ticker(i)
            ftx_list[f'{i}'] = res
            ftx_tmp = res['symbol']
            ftx_list_symbols.append(ftx_tmp)
        except:
            pass

    for i in coins_all:
        if i in ftx_list_symbols:
            symbol = ftx_list[f'{i}']['symbol']
            price = ftx_list[f'{i}']['last']
            ftx_tmp_ = price
            list_ftx.append(ftx_tmp_)
        else:
            symbol = i
            price = np.nan
            ftx_tmp_ = price
            list_ftx.append(ftx_tmp_)
    # print('FTX: ', list_ftx)
    return list_ftx


'''huobi'''
def huobi():
    global list_huobi
    huobi = ccxt.huobi()
    # print(huobi.fetch_tickers())
    huobi_list = {}
    huobi_list_symbols = []
    list_huobi = []

    for i in coins_all:
        try:
            res = huobi.fetch_ticker(i)
            huobi_list[f'{i}'] = res
            huobi_tmp = res['symbol']
            huobi_list_symbols.append(huobi_tmp)
        except:
            pass

    for i in coins_all:
        if i in huobi_list_symbols:
            symbol = huobi_list[f'{i}']['symbol']
            price = huobi_list[f'{i}']['last']
            huobi_tmp_ = price
            list_huobi.append(huobi_tmp_)
        else:
            symbol = i
            price = np.nan
            huobi_tmp_ = price
            list_huobi.append(huobi_tmp_)
    # print('HUOBI: ', list_huobi)
    return list_huobi


'''kucoin'''
def kucoin():
    global list_kucoin
    kucoin = ccxt.kucoin()
    # print(kucoin.fetch_tickers())
    kucoin_list = {}
    kucoin_list_symbols = []
    list_kucoin = []

    for i in coins_all:
        try:
            res = kucoin.fetch_ticker(i)
            kucoin_list[f'{i}'] = res
            kucoin_tmp = res['symbol']
            kucoin_list_symbols.append(kucoin_tmp)
        except:
            pass

    for i in coins_all:
        if i in kucoin_list_symbols:
            symbol = kucoin_list[f'{i}']['symbol']
            price = kucoin_list[f'{i}']['last']
            kucoin_tmp_ = price
            list_kucoin.append(kucoin_tmp_)
        else:
            symbol = i
            price = np.nan
            kucoin_tmp_ = price
            list_kucoin.append(kucoin_tmp_)
    # print('KUCOIN: ', list_kucoin)
    return list_kucoin

'''mexc'''
def mexc():
    global list_mexc
    mexc = ccxt.mexc()
    # print(mexc.fetch_tickers())
    mexc_list = {}
    mexc_list_symbols = []
    list_mexc = []

    for i in coins_all:
        try:
            res = mexc.fetch_ticker(i)
            mexc_list[f'{i}'] = res
            mexc_tmp = res['symbol']
            mexc_list_symbols.append(mexc_tmp)
        except:
            pass

    for i in coins_all:
        if i in mexc_list_symbols:
            symbol = mexc_list[f'{i}']['symbol']
            price = mexc_list[f'{i}']['last']
            mexc_tmp_ = price
            list_mexc.append(mexc_tmp_)
        else:
            symbol = i
            price = np.nan
            mexc_tmp_ = price
            list_mexc.append(mexc_tmp_)
    # print('MEXC: ', list_mexc)
    return list_mexc

'''phemex'''
def phemex():
    global list_phemex
    phemex = ccxt.phemex()
    # print(phemex.fetch_tickers())
    phemex_list = {}
    phemex_list_symbols = []
    list_phemex = []

    for i in coins_all:
        try:
            res = phemex.fetch_ticker(i)
            phemex_list[f'{i}'] = res
            phemex_tmp = res['symbol']
            phemex_list_symbols.append(phemex_tmp)
        except:
            pass

    for i in coins_all:
        if i in phemex_list_symbols:
            symbol = phemex_list[f'{i}']['symbol']
            price = phemex_list[f'{i}']['last']
            phemex_tmp_ = price
            list_phemex.append(phemex_tmp_)
        else:
            symbol = i
            price = np.nan
            phemex_tmp_ = price
            list_phemex.append(phemex_tmp_)
    # print('PHEMEX: ', list_phemex)
    return list_phemex

async def start_func():

    t1 = Thread(target=bybit)
    t4 = Thread(target=binance)
    t14 = Thread(target=ftx)
    t16 = Thread(target=huobi)
    t18 = Thread(target=kucoin)
    t19 = Thread(target=mexc)
    t20 = Thread(target=phemex)


    t1.start()
    t4.start()
    t14.start()
    t16.start()
    t18.start()
    t19.start()
    t20.start()


    t1.join()
    t4.join()
    t14.join()
    t16.join()
    t18.join()
    t19.join()
    t20.join()

    for i in coins:
        date_tmp = datetime.datetime.now()
        cur.execute('select count(*) from tickers')
        id = cur.fetchone()[0] + 1
        cur.execute(f"insert into tickers(id, coin_name, date_time) values ({id}, '{i}', '{date_tmp}')")
        con.commit()


    for i in list_binance:
        if i != None:
            cur.execute('select count(binance) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set binance = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(binance) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set binance = '{nan}' where id = {id}")
            con.commit()


    for i in list_bybit:
        if i != None:
            cur.execute('select count(bybit) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set bybit = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(bybit) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set bybit = '{nan}' where id = {id}")
            con.commit()

    for i in list_ftx:
        if i != None:
            cur.execute('select count(ftx) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set ftx = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(ftx) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set ftx = '{nan}' where id = {id}")
            con.commit()


    for i in list_huobi:
        if i != None:
            cur.execute('select count(huobi) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set huobi = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(huobi) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set huobi = '{nan}' where id = {id}")
            con.commit()


    for i in list_kucoin:
        if i != None:
            cur.execute('select count(kucoin) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set kucoin = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(kucoin) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set kucoin = '{nan}' where id = {id}")
            con.commit()


    for i in list_phemex:
        if i != None:
            cur.execute('select count(phemex) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set phemex = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(phemex) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set phemex = '{nan}' where id = {id}")
            con.commit()


    for i in list_mexc:
        if i != None:
            cur.execute('select count(mexc) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set mexc = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(mexc) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set mexc = '{nan}' where id = {id}")
            con.commit()



    # df = pd.DataFrame(data)
    # df.set_index('Coin', inplace=True)
    # df.to_excel('tickers_data.xlsx')
    # data_1 = df.idxmax(axis=1)
    # data_2 = df.idxmin(axis=1)
    # data_spread = ((df.max(axis=1) / df.min(axis=1))*100-100)
    # data_min = df.min(axis=1)
    # data_max = df.max(axis=1)
    # data_results = {'max_value': data_max, 'min_value': data_min, 'max_exchange':data_1, 'min_exchange':data_2, 'spread':data_spread}
    # dff = pd.DataFrame(data_results)
    # # print(dff)
    # dff.groupby(['spread']).max()
    # print(dff.sort_values(by='spread', ascending=False))
    # dff.to_excel('spread.xlsx')
    # end_time = datetime.datetime.now()
    # print(start_time, end_time)
    # data_tmp = df.to_json()
    # # print(data_tmp)
    # items = json.loads(data_tmp)
    # print(items)
    # for i in items:
    #     print(items[i])
