from aiogram.utils import executor
from create_cross import dp
from postgresql_cross import con, cur

async def on_startup(_):
    print('Bot is online')

import commands
cur.execute(f'CREATE TABLE tickers(id bigint primary key, coin_name text, '
            f'binance text, bybit text, ftx text, huobi text, kucoin text, mexc text, phemex text, date_time text)')
con.commi()

commands.register_handlers_client_partners(dp)



executor.start_polling(dp, skip_updates=True, on_startup=on_startup)