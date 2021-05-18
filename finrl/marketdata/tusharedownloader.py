"""Contains methods and classes to collect data from
tushare API
"""

import pandas as pd
import tushare as ts
from tqdm import tqdm

class tushareDownloader:
    """Provides methods for retrieving daily stock data from
    tushare API

    Attributes
    ----------
        start_date : str
            start date of the data (modified from config.py)
        end_date : str
            end date of the data (modified from config.py)
        ticker_list : list
            a list of stock tickers (modified from config.py)

    Methods
    -------
    fetch_data()
        Fetches data from tushare API

    """

    def __init__(self, start_date: str, end_date: str, ticker_list: list):

        self.start_date = start_date
        self.end_date = end_date
        self.ticker_list = ticker_list
        
    def fetch_data(self) -> pd.DataFrame:
        """Fetches data from Yahoo API
        Parameters
        ----------

        Returns
        -------
        `pd.DataFrame`
            7 columns: A date, open, high, low, close, volume and tick symbol
            for the specified stock ticker
        """
        # Download and save the data in a pandas DataFrame:
        data_df = pd.DataFrame()
        for tic in  tqdm(self.ticker_list, total=len(self.ticker_list)):
            temp_df = ts.get_hist_data(tic,start=self.start_date,end=self.end_date)
            temp_df["tic"] = tic
            data_df = data_df.append(temp_df)
        data_df = data_df.reset_index(level="date")
        try:
            # convert the column names to standardized names
            data_df.columns = [
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "price_change",
                "p_change",
                "ma5",
                "ma10",
                "ma20",
                "v_ma5",
                "v_ma10",
                "v_ma20",
                "turnover",
                "tic"
            ]
            data_df = data_df.drop(["price_change","p_change","ma5","ma10","ma20","v_ma5","v_ma10","v_ma20"], 1)
        except NotImplementedError:
            print("the features are not supported currently")
        # create day of the week column (monday = 0)
        
        data_df["day"] =  pd.to_datetime(data_df["date"]).dt.dayofweek
        #rank desc
        data_df = data_df.sort_index(axis=0,ascending=False)
        data_df = data_df.reset_index(drop=True)
        # convert date to standard string format, easy to filter
        data_df["date"] = pd.to_datetime(data_df["date"])
        data_df["date"] = data_df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
        # drop missing data
        data_df = data_df.dropna()
        print("Shape of DataFrame: ", data_df.shape)
        # print("Display DataFrame: ", data_df.head())

        data_df = data_df.sort_values(by=['date','tic']).reset_index(drop=True)

        return data_df

    def select_equal_rows_stock(self, df):
        df_check = df.tic.value_counts()
        df_check = pd.DataFrame(df_check).reset_index()
        df_check.columns = ["tic", "counts"]
        mean_df = df_check.counts.mean()
        equal_list = list(df.tic.value_counts() >= mean_df)
        names = df.tic.value_counts().index
        select_stocks_list = list(names[equal_list])
        df = df[df.tic.isin(select_stocks_list)]
        return df

    def select_equal_rows_stock(self, df):
        df_check = df.tic.value_counts()
        df_check = pd.DataFrame(df_check).reset_index()
        df_check.columns = ["tic", "counts"]
        mean_df = df_check.counts.mean()
        equal_list = list(df.tic.value_counts() >= mean_df)
        names = df.tic.value_counts().index
        select_stocks_list = list(names[equal_list])
        df = df[df.tic.isin(select_stocks_list)]
        return df
