import pandas as pd
import quantkit.data_sources.quandl as quandl


def get_quandl_fundamental_data(
    ticker: list,
    start_date: str = None,
    end_date: str = None,
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl Fundamental data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    start_date: str, optional
        date in format "yyyy-mm-dd"
    end_date: str, optional
        date in format "yyyy-mm-dd"
    columns: list, optional
        subset of columns to pull from table

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    api_key = "MxE6oNePp886npLJ2CGs"
    table = "SHARADAR/SF1"
    filters["ticker"] = ticker
    filters["dimension"] = "MRT"
    filters["paginate"] = True
    if start_date and end_date:
        filters["calendardate"] = {"gte": start_date, "lte": end_date}
    elif start_date:
        filters["calendardate"] = {"gte": start_date}
    elif end_date:
        filters["calendardate"] = {"lte": end_date}
    if columns:
        qopts = {"columns": columns}
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, table, filters)
    quandl_object.load()
    return quandl_object.df


def get_quandl_price_data(
    ticker: list,
    start_date: str = None,
    end_date: str = None,
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl price data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    start_date: str, optional
        date in format "yyyy-mm-dd"
    end_date: str, optional
        date in format "yyyy-mm-dd"
    columns: list, optional
        subset of columns to pull from table

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    api_key = "MxE6oNePp886npLJ2CGs"
    table = "SHARADAR/SEP"
    filters = dict()
    filters["ticker"] = ticker
    filters["paginate"] = True
    if start_date and end_date:
        filters["date"] = {"gte": start_date, "lte": end_date}
    elif start_date:
        filters["date"] = {"gte": start_date}
    elif end_date:
        filters["date"] = {"lte": end_date}
    if columns:
        qopts = {"columns": columns}
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, table, filters)
    quandl_object.load()
    return quandl_object.df
