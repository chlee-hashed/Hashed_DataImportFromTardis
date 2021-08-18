import asyncio
import aiohttp
import json
import urllib.parse
import pandas as pd
import time
import os
import datetime as dt





tardis_info_dic = {
    'binance':
    {
        'btcusdt' :
            {
                'start_date': '2019-04-01'
            },
        'ethusdt' :
            {
                'start_date' : '2019-04-01'
            },
        'xrpusdt' :
            {
                'start_date' : '2019-04-01'
            },
        'eosusdt' :
            {
                'start_date' : '2019-04-01'
            },
        'bnbusdt' :
            {
                'start_date' : '2019-04-01'
            },
        'adausdt' :
            {
                'start_date' : '2019-04-01'
            },
        'dogeusdt' :
            {
                'start_date' : '2019-07-05'
            },
        'busdusdt' :
            {
                'start_date' : '2019-11-08'
            },
        'dotusdt' :
            {
                'start_date' : '2020-08-18'
            },
        'maticusdt' :
            {
                'start_date' :'2019-11-07'
            },
        'linkusdt' :
            {
                'start_date' : '2019-11-07'
            },
        'solusdt' :
            {
                'start_date' : '2020-08-11'
            }


    },
    'binance-futures':
    {
        'btcusdt_210326' :
            {
                'start_date' : '2021-02-03',
                'end_date' : '2021-03-27'
            },
        'btcusdt_210625' :
            {
                'start_date' : '2021-03-16',
                'end_date' : '2021-06-25'
            },
        # 'btcusdt_210924' :
        #     {
        #         'start_date' : '2021-06-01',
        #         'end_date' : '2021-09-24'
        #     }
    },
    'bitmex':
    {
        'XBTU21' :
            {
                'start_date' : '2021-03-17',
                'end_date' : '2021-06-26'
            },
        'XBTM21':
            {
                'start_date' : '2021-05-15',
                'end_date' : '2021-09-25'
            }
    }
}


async def run(**kwargs):

    replay_option_dic = {}
    replay_option_dic['exchange'] = kwargs.get('exchange')
    replay_option_dic['from'] = kwargs.get('from')
    replay_option_dic['to'] = kwargs.get('to')
    replay_option_dic['symbols'] = [kwargs.get('symbol')]
    replay_option_dic['dataTypes'] = ['trade_bar_1m']

    replay_options =[replay_option_dic]


    options = urllib.parse.quote_plus(json.dumps(replay_options))
    URL = f"ws://localhost:8001/ws-replay-normalized?options={options}"

    # print(replay_option_dic['symbol'])
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(URL) as websocket:
            prev_date = None
            async for msg in websocket:
                raw_data_dict = json.loads(msg.data)
                _date = raw_data_dict['timestamp'][0:10]
                if prev_date == None:
                    print("START")
                    data_dic = {}
                    data_dic['open_price'] = {}
                    data_dic['high_price'] = {}
                    data_dic['low_price'] = {}
                    data_dic['close_price'] = {}
                    data_dic['vol'] = {}
                    prev_date = _date
                elif prev_date != _date:
                    print(_date)
                    print_df = pd.DataFrame(data_dic)
                    print_df.to_csv(file_path + '/' + prev_date + '.csv')
                    prev_date = _date
                    data_dic['open_price'] = {}
                    data_dic['high_price'] = {}
                    data_dic['low_price'] = {}
                    data_dic['close_price'] = {}
                    data_dic['vol'] = {}

                data_dic['open_price'][raw_data_dict['timestamp']] = raw_data_dict['open']
                data_dic['high_price'][raw_data_dict['timestamp']] = raw_data_dict['high']
                data_dic['low_price'][raw_data_dict['timestamp']] = raw_data_dict['low']
                data_dic['close_price'][raw_data_dict['timestamp']] = raw_data_dict['close']
                data_dic['vol'][raw_data_dict['timestamp']] = raw_data_dict['volume']
            if prev_date !=None:
                print_df = pd.DataFrame(data_dic)
                print_df.to_csv(file_path + '/' + _date + '.csv')

def FindLoadStartDate(check_start_date, file_path):
     files = os.listdir(file_path)
     start_date = check_start_date
     while(True):
         if start_date + '.csv' in files:
             start_date = (dt.datetime.strptime(start_date, '%Y-%m-%d') + dt.timedelta(days=1)).strftime('%Y-%m-%d')
         else:
             break

     return start_date








if __name__ == "__main__" :

    #### Set Parm ####

    exchange_list_for_download = ['binance','binance-futures','bitmex']
    item_type_dic = {'binance':'spot','binance-futures':'futures','bitmex':'futures'}


    start_date_str = (dt.date.today() + dt.timedelta(-2)).strftime(('%Y-%m-%d'))
    calc_date_str= (dt.date.today() + dt.timedelta(-1)).strftime(('%Y-%m-%d'))

    ##################


    for exchange_name in exchange_list_for_download:
        id_list = tardis_info_dic[exchange_name].keys()

        for _id in id_list:

            file_path = 'd:/data/min/' + exchange_name + '/' + item_type_dic[exchange_name] + '/' + _id
            if os.path.isdir(file_path) == False:
                os.makedirs(file_path)

            kwargs={}
            kwargs['exchange'] = exchange_name
            kwargs['from'] = FindLoadStartDate(tardis_info_dic[exchange_name][_id]['start_date'],file_path)
            kwargs['to'] = calc_date_str
            kwargs['symbol'] = _id
            kwargs['dataTypes'] = 'trade_bar_1m'
            kwargs['path'] = file_path
            print(exchange_name, kwargs['symbol'], kwargs['from'])

            asyncio.run(run(**kwargs))





