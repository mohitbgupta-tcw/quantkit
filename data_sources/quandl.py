import sys, os

sys.path.append(os.getcwd())

import pandas as pd
from copy import deepcopy
import nasdaqdatalink
import quantkit.utils.util_functions as util_functions


class Quandl(object):
    """
    Main class to load Quandl data using the Quandl API

    Parameters
    ----------
    key: str
        quandl api key
    table: str
        table name
    filters: dict
        dictionary of parameters for function call
    """

    def __init__(self, key: str, table: str, filters: dict):
        self.key = key
        self.table = table
        self.filters = filters

    def load(self):
        """
        Load data from quandl API and save as pd.DataFrame in self.df
        """
        nasdaqdatalink.ApiConfig.api_key = self.key
        nasdaqdatalink.ApiConfig.verify_ssl = "certs.crt"

        batches = list(util_functions.divide_chunks(self.filters["ticker"], 400))
        self.df = pd.DataFrame()
        for batch in batches:
            filters = deepcopy(self.filters)
            filters["ticker"] = batch

            df = nasdaqdatalink.get_table(self.table, **filters)
            self.df = pd.concat([self.df, df], ignore_index=True)
        return


if __name__ == "__main__":
    api_key = "MxE6oNePp886npLJ2CGs"
    table = "SHARADAR/SF1"
    filters = {
        "ticker": ["AAPL", "MSFT", "TSLA"],
        "dimension": "MRT",
        "calendardate": {"gte": "2023-01-01"},
        "paginate": True,
    }

    quandl = Quandl(api_key, table, filters)
    quandl.load()
    print(quandl.df)
