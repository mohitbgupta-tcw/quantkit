import pandas as pd
from fredapi import Fred as Fred_API


class FRED(object):
    """
    Main class to load FRED data using the FRED API

    Parameters
    ----------
    key: str
        fred api key
    tickers: list
        list of ticker names from FRED
    revision: revision
        take revised series
    filters: dict
        dictionary of parameters for API call
    """

    def __init__(
        self, key: str, tickers: list, revision: bool, filters: dict = {}, **kwargs
    ) -> None:
        self.key = key
        self.tickers = tickers
        self.revision = revision
        self.filters = filters

    def load(self, **kwargs) -> None:
        """
        Load data from FRED API and save as pd.DataFrame in self.df
        """
        fred = Fred_API(api_key=self.key)
        if self.revision:
            self.df = pd.DataFrame()
            for series in self.tickers:
                if "realtime_start" in self.filters:
                    df = fred.get_series_all_releases(
                        series, realtime_start=self.filters["realtime_start"]
                    )
                else:
                    df = fred.get_series_all_releases(series)
                df["id"] = series
                self.df = pd.concat([self.df, df])
        else:
            self.df = pd.DataFrame(
                {series: fred.get_series(series) for series in self.tickers}
            )
