import nasdaqdatalink


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
        self.load()

    def load(self):
        """
        Load data from quandl API and save as pd.DataFrame in self.df
        """
        nasdaqdatalink.ApiConfig.api_key = self.key
        nasdaqdatalink.ApiConfig.verify_ssl = "certs.crt"
        df = nasdaqdatalink.get_table(self.table, **self.filters)
        self.df = df
        return


if __name__ == "__main__":
    api_key = "MxE6oNePp886npLJ2CGs"
    table = "SHARADAR/SF1"
    filters = {
        "ticker": ["AAPL", "CMI", "MSFT"],
        "dimension": {"MRT"},
        "calendardate": {"gte": "2023-01-01", "lte": "2023-03-31"},
        "paginate": True,
    }

    quandl = Quandl(api_key, table, filters)
    print(quandl.df)
