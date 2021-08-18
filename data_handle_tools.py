import numpy as np
import pandas as pd
import os




def gen_min_candle_regular_form(df):
    df['dt'] = pd.to_datetime(df[df.columns[0]])
    df['date'] = df['dt'].map(lambda x: x.date().strftime('%Y-%m-%d'))
    df['time'] = df['dt'].map(lambda x: x.time())
    df = df[['date', 'time', 'open_price', 'high_price', 'low_price', 'close_price', 'vol']]
    return df

def gen_min_candle_in_one_file(_folder_path, regular_file_format):
    output_df = pd.DataFrame()
    files = os.listdir(_folder_path)
    last_file_name = None

    for _file in files:
        if _file[-3:] != 'csv':
            continue
        else:
            last_file_name = _file[:-4]
        df = pd.read_csv(_folder_path + _file)
        if regular_file_format:
            df = gen_min_candle_regular_form(df)
        if len(output_df) == 0:
            output_df = df
        else:
            output_df = pd.concat((output_df, df), sort= False)

        output_df.reset_index(drop= True, inplace= True)

    return output_df, last_file_name




if __name__ == "__main__" :

    folder_path = 'd:/data/min/binance/spot/btcusdt/'
    output_df, last_date = gen_min_candle_in_one_file(folder_path,regular_file_format = True)
    output_path = folder_path + 'regular_form/'
    print(output_path)
    if os.path.isdir((output_path)) == False:
        os.makedirs((output_path))
    name_set = folder_path.split('/')
    output_df.to_csv(output_path + name_set[3] + '_'+name_set[-2]+ '_'+last_date+ '.csv')