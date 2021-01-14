# imports

import os
import shutil
import string
import time  # used to measure execution time
from multiprocessing import Pool

import pandas as pd

trainingset_folder = "D:/data_mt/09_training/"
stock_data_folder = trainingset_folder + "stocks/"
combine_data_folder = trainingset_folder + "combined/"

overwrite = True

def create_dir_structure(startfolder: str):
    if not os.path.exists(startfolder):
        os.makedirs(startfolder)

    for char in string.ascii_uppercase:
        folder = startfolder + char + "/"
        directory = os.path.dirname(folder)
        if not os.path.exists(directory):
            os.makedirs(directory)


def load_additional_info() -> pd.DataFrame:
    return pd.read_csv(trainingset_folder + "company_info.csv", sep=',', encoding='utf-8', header=0)


def load_reports():
    df = pd.read_csv(trainingset_folder + "company_reports.csv", header=0)
    df.period = pd.to_datetime(df.period)
    df.filed = pd.to_datetime(df.filed)
    return df


def load_stock_history(ticker: str):
    df = pd.read_csv(stock_data_folder + ticker[0] + "/" + ticker + ".csv")
    df.Date = pd.to_datetime(df.Date)
    return df


def merge_dataframes(ticker, ticker_fd_data):
    ticker_stock_data = load_stock_history(ticker)
    ticker_stock_data = ticker_stock_data[ticker_stock_data.Date > "2012-01-01"]
    ticker_stock_data['i_date'] = ticker_stock_data.Date

    ticker_stock_data.set_index('i_date', inplace=True)
    ticker_fd_data['i_date'] = ticker_fd_data.filed
    ticker_fd_data.set_index('i_date', inplace=True)

    combined_data = pd.merge(ticker_fd_data, ticker_stock_data, left_index=True, right_index=True, how='outer')
    combined_data.sort_index(inplace=True)

    combined_data = combined_data.fillna(method="ffill")
    combined_data = combined_data.dropna(subset=['filed', 'Date'])

    combined_data['ticker'] = combined_data.ticker_x
    combined_data.drop(columns=['ticker_x', 'ticker_y'])

    return combined_data


def create_price_ratio_features(combined_data, shares_outstanding):
    combined_data['pr_p2e'] = combined_data.Close * shares_outstanding / (
            combined_data.c_NetIncomeLoss - combined_data.c_PaymentsOfDividendsTotal)
    combined_data['pr_p2b'] = combined_data.Close * shares_outstanding / (
            combined_data.Assets - combined_data.Liabilities)
    combined_data['pr_p2egr_1y'] = combined_data.pr_p2e / (
            combined_data.gr_netincome_p * 100)  # approximated / denominator in percent

    # caping p2e: in order to prevent meaningless values, we need to restrict the range. The max value is limited to 100.
    # if new_df.c_NetIncomeLoss - new_df.c_PaymentsOfDividendsTotal results in a negativ value, we set p2e to 100, which is rather a "bad" value.
    combined_data.loc[(combined_data.pr_p2e < 0) | (combined_data.pr_p2e > 100), 'pr_p2e'] = 100
    # caping p2egr: the lower the better. generally you would like to see a ratio lower than 1, so a 5 could be a really bad value so we restrict it to 5
    # if growth number is 0 or less, we set p2egr to 5
    combined_data.loc[(combined_data.pr_p2egr_1y > 5) | (combined_data.pr_p2egr_1y <= 0.0), 'pr_p2egr_1y'] = 5


def find_10_day_max(date, close, df):
    date_low = date + pd.DateOffset(days=180)
    date_high =  date + pd.DateOffset(days=360)

    # return df[(df.Date >= date_low) & ((df.Date <= date_high))].Close.max()
    close_list = df[(df.Date >= date_low) & (df.Date <= date_high)].Close.sort_values(ascending=False).to_list()
    if len(close_list) >= 10:
        return close_list[9]
    return 0


def calculate_potential(combined_data):
    combined_data['c_max_10day'] = combined_data.apply(lambda row : find_10_day_max(row['Date'], row['Close'], combined_data), axis = 1)
    combined_data['r_potential'] = (combined_data.c_max_10day / combined_data.Close) - 1


def process_ticker(data_tuple):
    try:
        ticker          = data_tuple[0]
        ticker_fd_data  = data_tuple[1]
        ticker_add_info = data_tuple[2]

        new_file = combine_data_folder + ticker[0] + "/" + ticker + ".csv"

        print('process: ', ticker, end="")
        if os.path.isfile(new_file) & (overwrite is False):
            print(" skip")
            return

        print("...")

        shares_outstanding = ticker_add_info.sharesOutstanding.to_list()[0]

        combined_data = merge_dataframes(ticker, ticker_fd_data)
        create_price_ratio_features(combined_data, shares_outstanding)
        calculate_potential(combined_data)

        combined_data.to_csv(new_file, sep=',', encoding='utf-8', index=False)
    except Exception as e:
        print(e)

        
def data_generator():
    add_info = load_additional_info()
    fd_data = load_reports()
    tickers = add_info.ticker.unique()
    print(len(tickers))

    for ticker in tickers:
        ticker_fd_data = fd_data[fd_data.ticker == ticker].copy()
        ticker_add_info = add_info[add_info.ticker == ticker]

        yield ticker, ticker_fd_data, ticker_add_info


if __name__ == '__main__':
    if overwrite:
        shutil.rmtree(combine_data_folder,ignore_errors = True)

    create_dir_structure(combine_data_folder)

    start = time.time()
    # serial
    # for data_tuple in data_generator():
    #     process_ticker(data_tuple)


    #parallel
    # needs about 30 minutes...
    pool = Pool(8)
    pool.map(process_ticker, data_generator())
    pool.close()
    pool.join()

    print("duration: ", time.time() - start)

