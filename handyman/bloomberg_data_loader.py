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
        list of bloomberg tickers to run in API, p.e. ["AAPL US EQUITY", "MSFT US Equity"]
    start_date: str, optional
        date in format "yyyy-mm-dd"
    end_date: str, optional
        date in format "yyyy-mm-dd"

    Returns
    -------
    pd.DataFrame
        DataFrame of Bloomberg information
    """
    filters = dict()
    filters["tickers"] = tickers

    if start_date:
        filters["start_date"] = start_date
    elif end_date:
        filters["end_date"] = end_date

    bloomberg_object = bloomberg.Bloomberg("prices", filters)
    bloomberg_object.load()

    df = bloomberg_object.df
    df.columns = df.columns.droplevel(level=1)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index(ascending=True)
    return df


def get_fundamental_data(
    tickers: list,
    fields: list,
    years: list,
    report_period: str = "QUARTER",
) -> pd.DataFrame:
    """
    For a specified list of securities, load Bloomberg price data through API

    Parameters
    ----------
    ticker: list
        list of bloomberg tickers to run in API, p.e. ["AAPL US EQUITY", "MSFT US Equity"]
    fields: list
        list of fields from balance. financial sheet to pull
    years: list
        years to pull data for
    report_period: str, optional
        report period, either 'QUARTER', 'YEAR'

    Returns
    -------
    pd.DataFrame
        DataFrame of Bloomberg information
    """
    fields.extend(["ANNOUNCEMENT_DT", "LATEST_ANNOUNCEMENT_PERIOD"])
    filters = dict()
    filters["tickers"] = tickers
    filters["flds"] = fields
    filters["years"] = years
    filters["report_period"] = report_period

    bloomberg_object = bloomberg.Bloomberg("fundamental", filters)
    bloomberg_object.load()

    df = bloomberg_object.df
    df.index.name = "Ticker"
    df["announcement_dt"] = pd.to_datetime(df["announcement_dt"])
    df = df.sort_values(by=["Ticker", "latest_announcement_period"], ascending=True)
    return df
