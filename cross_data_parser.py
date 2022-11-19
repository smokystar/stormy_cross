import ccxt
import datetime
from threading import Thread
import pandas as pd
from const import coins_all, coins_bybit, coins
import numpy as np
from create_cross import bot
import json
from postgresql_cross import con, cur

admin_id = 394652149
vova_id = 323039084
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


'''huobi'''


def huobi():
    global list_huobi
    huobi = ccxt.mexc()
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


def gateio():
    global list_gateio
    gateio = ccxt.gateio()
    # print(gateio.fetch_tickers())
    gateio_list = {}
    gateio_list_symbols = []
    list_gateio = []

    for i in coins_all:
        try:
            res = gateio.fetch_ticker(i)
            gateio_list[f'{i}'] = res
            gateio_tmp = res['symbol']
            gateio_list_symbols.append(gateio_tmp)
        except:
            pass

    for i in coins_all:
        if i in gateio_list_symbols:
            symbol = gateio_list[f'{i}']['symbol']
            price = gateio_list[f'{i}']['last']
            gateio_tmp_ = price
            list_gateio.append(gateio_tmp_)
        else:
            symbol = i
            price = np.nan
            gateio_tmp_ = price
            list_gateio.append(gateio_tmp_)
    # print('GATEIO: ', list_gateio)
    return list_gateio


def bitget():
    global list_bitget
    bitget = ccxt.bitget()
    # print(bitget.fetch_tickers())
    bitget_list = {}
    bitget_list_symbols = []
    list_bitget = []

    for i in coins_all:
        try:
            res = bitget.fetch_ticker(i)
            bitget_list[f'{i}'] = res
            bitget_tmp = res['symbol']
            bitget_list_symbols.append(bitget_tmp)
        except:
            pass

    for i in coins_all:
        if i in bitget_list_symbols:
            symbol = bitget_list[f'{i}']['symbol']
            price = bitget_list[f'{i}']['last']
            bitget_tmp_ = price
            list_bitget.append(bitget_tmp_)
        else:
            symbol = i
            price = np.nan
            bitget_tmp_ = price
            list_bitget.append(bitget_tmp_)
    # print('BITGET: ', list_bitget)
    return list_bitget





async def start_func():
    t1 = Thread(target=bybit)
    t2 = Thread(target=binance)
    t4 = Thread(target=huobi)
    t5 = Thread(target=kucoin)
    t9 = Thread(target=bitget)
    t14 = Thread(target=gateio)

    t1.start()
    t2.start()

    t4.start()
    t5.start()
    t9.start()
    t14.start()

    t1.join()
    t2.join()

    t4.join()
    t5.join()
    t9.join()

    t14.join()

    data = {'Coin': coins, 'binance': list_binance,
            'bybit': list_bybit, 'bitget': list_bitget, 'gateio': list_gateio,
            'huobi': list_huobi, 'kucoin': list_kucoin}

    df = pd.DataFrame(data)
    df.set_index('Coin', inplace=True)
    # print(df.head(10))
    data_1 = df.idxmax(axis=1)
    data_2 = df.idxmin(axis=1)
    data_spread = ((df.max(axis=1) / df.min(axis=1)) * 100 - 100)
    data_min = df.min(axis=1)
    data_max = df.max(axis=1)
    data_results = {'max_value': data_max, 'min_value': data_min, 'max_exchange': data_1, 'min_exchange': data_2,
                    'spread': data_spread}
    dff = pd.DataFrame(data_results)
    dff.sort_values(by='spread', ascending=False, inplace=True)
    # print(dff)
    dict = dff.to_json()
    tmp = dff[dff['spread'] > 0.4]
    trigger = dff['spread'].max()
    # print(trigger)
    if trigger > 0.4:
        await bot.send_message(admin_id, f'{tmp}')
        await bot.send_message(vova_id, f'{tmp}')
    # for i in coins:
    #     max = dict['max_value'][i]
    #     min = dict['min_value'][i]
    #     max_e = dict['max_exchange'][i]
    #     min_e = dict['min_exchange'][i]
    #     spread = dict['spread'][i]
    #     print(f'Buy coin: {i} exchange: {min_e} price: {min} -> sell exchange: {max_e} price: {max} ==> SPREAD: {spread}')
    #     # await bot.send_message(admin_id, f'Buy coin: {i} exchange: {min_e} price: {min} -> sell exchange: {max_e} price: {max} ==> SPREAD: {spread}')

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



    for i in list_gateio:
        if i != None:
            cur.execute('select count(gateio) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set gateio = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(gateio) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set gateio = '{nan}' where id = {id}")
            con.commit()

    for i in list_bitget:
        if i != None:
            cur.execute('select count(bitget) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set bitget = '{i}' where id = {id}")
            con.commit()
        else:
            nan = 'None'
            cur.execute('select count(bitget) from tickers')
            id = cur.fetchone()[0] + 1
            cur.execute(f"update tickers set bitget = '{nan}' where id = {id}")
            con.commit()

