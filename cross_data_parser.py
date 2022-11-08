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


# def bitfinex():
#     global list_bitfinex
#     bitfinex = ccxt.bitfinex()
#     # print(bitfinex.fetch_tickers())
#     bitfinex_list = {}
#     bitfinex_list_symbols = []
#     list_bitfinex = []
# 
#     for i in coins_all:
#         try:
#             res = bitfinex.fetch_ticker(i)
#             bitfinex_list[f'{i}'] = res
#             bitfinex_tmp = res['symbol']
#             bitfinex_list_symbols.append(bitfinex_tmp)
#         except:
#             pass
# 
#     for i in coins_all:
#         if i in bitfinex_list_symbols:
#             symbol = bitfinex_list[f'{i}']['symbol']
#             price = bitfinex_list[f'{i}']['last']
#             bitfinex_tmp_ = price
#             list_bitfinex.append(bitfinex_tmp_)
#         else:
#             symbol = i
#             price = np.nan
#             bitfinex_tmp_ = price
#             list_bitfinex.append(bitfinex_tmp_)
#     # print('bitfinex: ', list_bitfinex)
#     return list_bitfinex


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


# def poloniex():
#     global list_poloniex
#     poloniex = ccxt.poloniex()
#     # print(poloniex.fetch_tickers())
#     poloniex_list = {}
#     poloniex_list_symbols = []
#     list_poloniex = []
#
#     for i in coins_all:
#         try:
#             res = poloniex.fetch_ticker(i)
#             poloniex_list[f'{i}'] = res
#             poloniex_tmp = res['symbol']
#             poloniex_list_symbols.append(poloniex_tmp)
#         except:
#             pass
#
#     for i in coins_all:
#         if i in poloniex_list_symbols:
#             symbol = poloniex_list[f'{i}']['symbol']
#             price = poloniex_list[f'{i}']['last']
#             poloniex_tmp_ = price
#             list_poloniex.append(poloniex_tmp_)
#         else:
#             symbol = i
#             price = np.nan
#             poloniex_tmp_ = price
#             list_poloniex.append(poloniex_tmp_)
#     # print('POLONIEX: ', list_poloniex)
#     return list_poloniex


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


def bitforex():
    global list_bitforex
    bitforex = ccxt.bitforex()
    # print(bitforex.fetch_tickers())
    bitforex_list = {}
    bitforex_list_symbols = []
    list_bitforex = []

    for i in coins_all:
        try:
            res = bitforex.fetch_ticker(i)
            bitforex_list[f'{i}'] = res
            bitforex_tmp = res['symbol']
            bitforex_list_symbols.append(bitforex_tmp)
        except:
            pass

    for i in coins_all:
        if i in bitforex_list_symbols:
            symbol = bitforex_list[f'{i}']['symbol']
            price = bitforex_list[f'{i}']['last']
            bitforex_tmp_ = price
            list_bitforex.append(bitforex_tmp_)
        else:
            symbol = i
            price = np.nan
            bitforex_tmp_ = price
            list_bitforex.append(bitforex_tmp_)
    # print('BITFOREX', list_bitforex)
    return list_bitforex

async def start_func():

    t1 = Thread(target=bybit)
    t2 = Thread(target=binance)
    t3 = Thread(target=ftx)
    t4 = Thread(target=huobi)
    t5 = Thread(target=kucoin)
    t6 = Thread(target=mexc)
    t7 = Thread(target=phemex)

    t9 = Thread(target=bitget)
    t10 = Thread(target=bitforex)

    t14 = Thread(target=gateio)


    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()

    t9.start()
    t10.start()

    t14.start()


    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()

    t9.join()
    t10.join()

    t14.join()

    data = {'Coin': coins, 'binance': list_binance,
            'bybit': list_bybit, 'bitforex': list_bitforex, 'bitget': list_bitget, 'ftx': list_ftx, 'gateio': list_gateio,
            'huobi': list_huobi, 'kucoin': list_kucoin, 'mexc': list_mexc, 'phemex': list_phemex}

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
    #print(dff)
    dict = dff.to_json()
    tmp = dff[dff['spread'] > 0.7]
    trigger = dff['spread'].max()
    #print(trigger)
    if trigger > 0.7:
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








    # for i in coins:
    #     date_tmp = datetime.datetime.now()
    #     cur.execute('select count(*) from tickers')
    #     id = cur.fetchone()[0] + 1
    #     cur.execute(f"insert into tickers(id, coin_name, date_time) values ({id}, '{i}', '{date_tmp}')")
    #     con.commit()
    #
    #
    # for i in list_binance:
    #     if i != None:
    #         cur.execute('select count(binance) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set binance = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(binance) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set binance = '{nan}' where id = {id}")
    #         con.commit()
    #
    #
    # for i in list_bybit:
    #     if i != None:
    #         cur.execute('select count(bybit) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bybit = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(bybit) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bybit = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_ftx:
    #     if i != None:
    #         cur.execute('select count(ftx) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set ftx = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(ftx) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set ftx = '{nan}' where id = {id}")
    #         con.commit()
    #
    #
    # for i in list_huobi:
    #     if i != None:
    #         cur.execute('select count(huobi) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set huobi = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(huobi) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set huobi = '{nan}' where id = {id}")
    #         con.commit()
    #
    #
    # for i in list_kucoin:
    #     if i != None:
    #         cur.execute('select count(kucoin) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set kucoin = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(kucoin) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set kucoin = '{nan}' where id = {id}")
    #         con.commit()
    #
    #
    # for i in list_phemex:
    #     if i != None:
    #         cur.execute('select count(phemex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set phemex = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(phemex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set phemex = '{nan}' where id = {id}")
    #         con.commit()
    #
    #
    # for i in list_mexc:
    #     if i != None:
    #         cur.execute('select count(mexc) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set mexc = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(mexc) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set mexc = '{nan}' where id = {id}")
    #         con.commit()
    #
    #
    # for i in list_bitforex:
    #     if i != None:
    #         cur.execute('select count(bitforex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bitforex = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(bitforex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bitforex = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_bitfinex:
    #     if i != None:
    #         cur.execute('select count(bitfinex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bitfinex = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(bitfinex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bitfinex = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_gateio:
    #     if i != None:
    #         cur.execute('select count(gateio) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set gateio = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(gateio) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set gateio = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_bithumb:
    #     if i != None:
    #         cur.execute('select count(bithumb) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bithumb = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(bithumb) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bithumb = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_upbit:
    #     if i != None:
    #         cur.execute('select count(upbit) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set upbit = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(upbit) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set upbit = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_poloniex:
    #     if i != None:
    #         cur.execute('select count(poloniex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set poloniex = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(poloniex) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set poloniex = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_aax:
    #     if i != None:
    #         cur.execute('select count(aax) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set aax = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(aax) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set aax = '{nan}' where id = {id}")
    #         con.commit()
    #
    # for i in list_bitget:
    #     if i != None:
    #         cur.execute('select count(bitget) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bitget = '{i}' where id = {id}")
    #         con.commit()
    #     else:
    #         nan = 'None'
    #         cur.execute('select count(bitget) from tickers')
    #         id = cur.fetchone()[0] + 1
    #         cur.execute(f"update tickers set bitget = '{nan}' where id = {id}")
    #         con.commit()

