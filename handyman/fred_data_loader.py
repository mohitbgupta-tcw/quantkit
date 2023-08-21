import pandas as pd
import quantkit.data_sources.fred as fred


def run_fred_api(tickers: list) -> pd.DataFrame:
    """
    Get economic datapoints through FRED API

    Parameters
    ----------
    ticker: list
        list of tickers from FRED
        (check available data here: https://fred.stlouisfed.org/)

    Returns
    -------
    pd.DataFrame
        DataFrame of FRED information
    """
    api_key = "eb4f46c42913c92951a8c64d545598ca"

    fred_object = fred.FRED(api_key, tickers=tickers)
    fred_object.load()
    return fred_object.df
