import pandas as pd
import quantkit.data_sources.quandl as quandl


def run_quandl_api(ticker: list = ["AAPL", "MSFT", "TSLA"]) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl Fundamental data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    api_key = "MxE6oNePp886npLJ2CGs"
    table = "SHARADAR/SF1"
    filters = {
        "ticker": ticker,
        "dimension": "MRT",
        "calendardate": {"gte": "2023-01-01"},
        "paginate": True,
    }

    quandl_object = quandl.Quandl(api_key, table, filters)
    quandl_object.load()
    return quandl_object.df
