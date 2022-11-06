from aiogram import types, Dispatcher
from create_cross import dp, bot
from buttons import key_menu_client, tasks_menu
from aiogram import types
from aiogram.dispatcher import Dispatcher
from postgresql_cross import con, cur
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import psycopg2
from cross_data_parser import start_func
import datetime
import time
import requests
import pandas as pd
import re


api_key = '1QrbAnjDYWcnmKoQYVn2ZSphucr4yXZtWEwUATG103rqfgJqG0VZ5kW7vdtMIS0Q'
secret_key = 'IU08Ye3WRhrjBEZl28vA9CN3TWL2fLSEv1XMZA8kYjmASbWOPpvVwhXfF6s6WQyS'
client = Client(api_key, secret_key)


from aiogram.dispatcher.filters import Text
admin_id = 394652149

n = 0

async def start_cmd(message: types.Message):
    await bot.send_message(message.from_user.id, 'Hello!', reply_markup=key_menu_client)


async def mainmenu(message: types.Message):
    await bot.send_message(message.from_user.id, 'Main Menu', reply_markup=tasks_menu)


async def cross_scan(message: types.Message):
    user_id = message.from_user.id
    print('start')
    while n < 10:
        try:
            await start_func()
            time.sleep(30)
        except Exception as e:
            await bot.send_message(message.from_user.id, f'Error occured!\n\n {e}')


def register_handlers_client_partners(dp: Dispatcher):
    dp.register_message_handler(start_cmd, commands=['start'])
    dp.register_message_handler(start_cmd, Text(equals='start', ignore_case=True))
    dp.register_message_handler(mainmenu, commands=['Main menu'])
    dp.register_message_handler(mainmenu, Text(equals='Main menu', ignore_case=True))
    dp.register_message_handler(cross_scan, commands=['activate'])
    dp.register_message_handler(cross_scan, Text(equals='activate', ignore_case=True))


