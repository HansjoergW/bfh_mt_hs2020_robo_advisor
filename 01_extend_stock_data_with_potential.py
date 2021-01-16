import os
import shutil
import string
import time  # used to measure execution time
from multiprocessing import Pool

import pandas as pd


trainingset_folder = "D:/data_mt/09_training/"
stock_data_folder = trainingset_folder + "stocks/"
stock_potential_folder = trainingset_folder + "stocks_w_potential/"

overwrite = True


def create_dir_structure(startfolder: str):
    if not os.path.exists(startfolder):
        os.makedirs(startfolder)

    for char in string.ascii_uppercase:
        folder = startfolder + char + "/"
        directory = os.path.dirname(folder)
        if not os.path.exists(directory):
            os.makedirs(directory)


def load_stock_history(ticker: str):
    df = pd.read_csv(stock_data_folder + ticker[0] + "/" + ticker + ".csv")
    df.Date = pd.to_datetime(df.Date)

    df['i_date'] = df.Date
    df.sort_index(inplace=True)
    return df

def find_10_day_max(date, close, df):
    date_low = date + pd.DateOffset(days=180)
    date_high =  date + pd.DateOffset(days=360)

    close_list = df[(df.Date >= date_low) & (df.Date <= date_high)].Close.sort_values(ascending=False).to_list()
    if len(close_list) >= 10:
        return close_list[9]
    return 0


def calculate_potential(stock_data):
    stock_data['c_max_10day'] = stock_data.apply(lambda row : find_10_day_max(row['Date'], row['Close'], stock_data), axis = 1)
    stock_data['r_potential'] = (stock_data.c_max_10day / stock_data.Close) - 1


def process_ticker(ticker):
    try:
        new_file = stock_potential_folder + ticker[0] + "/" + ticker + ".csv"

        print('process: ', ticker, end="")
        if os.path.isfile(new_file) & (overwrite is False):
            print(" skip")
            return

        print("...")

        stock_data = load_stock_history(ticker)
        calculate_potential(stock_data)

        stock_data.to_csv(new_file, sep=',', encoding='utf-8', index=False)
    except Exception as e:
        print(e)


def load_additional_info() -> pd.DataFrame:
    return pd.read_csv(trainingset_folder + "company_info.csv", sep=',', encoding='utf-8', header=0)


if __name__ == '__main__':
    create_dir_structure(stock_potential_folder)

    add_info = load_additional_info()
    tickers = add_info.ticker.unique()
    print(len(tickers))

    start = time.time()
    # serial
    # for data_tuple in data_generator():
    #     process_ticker(data_tuple)


    #parallel
    # needs about 30 minutes...
    pool = Pool(8)
    pool.map(process_ticker, tickers)
    pool.close()
    pool.join()

    print("duration: ", time.time() - start)