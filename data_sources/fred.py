import os
import pandas as pd
from copy import deepcopy
from fredapi import Fred as Fred_API


class FRED(object):
    """
    Main class to load FRED data using the FRED API

    Parameters
    ----------
    key: str
        quandl api key
    tickers: list
        list of ticker names from FRED
    """

    def __init__(self, key: str, tickers: list, **kwargs) -> None:
        self.key = key
        self.tickers = tickers

    def load(self, **kwargs) -> None:
        """
        Load data from FRED API and save as pd.DataFrame in self.df
        """
        fred = Fred_API(api_key=self.key)
        self.df = pd.DataFrame(
            {series: fred.get_series(series) for series in self.tickers}
        )
