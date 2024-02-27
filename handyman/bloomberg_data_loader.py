import quantkit.core.data_sources.bloomberg as bloomberg
import pandas as pd


def get_price_data(
    tickers: list,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Bloomberg price data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    start_date: str, optional
        date in format "yyyy-mm-dd"
    end_date: str, optional
        date in format "yyyy-mm-dd"

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    filters["tickers"] = tickers
    
    if start_date:
        filters["start_date"] = start_date
    elif end_date:
        filters["end_date"] = end_date

    bloomberg_object = bloomberg.Bloomberg("prices", filters)
    bloomberg_object.load()
    return bloomberg_object.df