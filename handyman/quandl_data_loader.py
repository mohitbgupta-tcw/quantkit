import pandas as pd
import quantkit.core.data_sources.quandl as quandl


def get_table_information(table: list) -> pd.DataFrame:
    """
    For a specified table, load Quandl Table information through API

    Parameters
    ----------
    table: str
        table name

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    api_key = "MxE6oNePp886npLJ2CGs"
    datatable_code = "SHARADAR/INDICATORS"
    filters["table"] = table

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    return quandl_object.df


def get_ticker_metadata(
    ticker: list,
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl Company data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    columns: list, optional
        subset of columns to pull from table

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    api_key = "MxE6oNePp886npLJ2CGs"
    datatable_code = "SHARADAR/TICKERS"
    filters["ticker"] = ticker
    filters["table"] = "SF1"
    qopts = dict()

    if columns:
        qopts["columns"] = columns

    if qopts:
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    return quandl_object.df


def get_daily_multiples_data(
    ticker: list,
    start_date: str = None,
    end_date: str = None,
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl mutliples data through API

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
    datatable_code = "SHARADAR/DAILY"
    filters["ticker"] = ticker

    qopts = dict()
    if start_date and end_date:
        filters["date"] = {"gte": start_date, "lte": end_date}
    elif start_date:
        filters["date"] = {"gte": start_date}
    elif end_date:
        filters["date"] = {"lte": end_date}
    if columns:
        qopts["columns"] = columns

    if qopts:
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    return quandl_object.df


def get_fundamental_data(
    ticker: list,
    latest: bool = False,
    start_date: str = None,
    end_date: str = None,
    report_period: str = "TRAILING",
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl Fundamental data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    latest: bool, optional
        Only pull latest data
    start_date: str, optional
        date in format "yyyy-mm-dd"
    end_date: str, optional
        date in format "yyyy-mm-dd"
    report_period: str, optional
        report period, either 'QUARTER', 'YEAR', 'TRAILING'
    columns: list, optional
        subset of columns to pull from table

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    api_key = "MxE6oNePp886npLJ2CGs"
    datatable_code = "SHARADAR/SF1"
    filters["ticker"] = ticker

    qopts = dict()
    if report_period == "QUARTER":
        filters["dimension"] = "ARQ"
    elif report_period == "QUARTER":
        filters["dimension"] = "ARY"
    else:
        filters["dimension"] = "ART"
    filters["paginate"] = True
    if latest:
        qopts["latest"] = 1
    elif start_date and end_date:
        filters["calendardate"] = {"gte": start_date, "lte": end_date}
    elif start_date:
        filters["calendardate"] = {"gte": start_date}
    elif end_date:
        filters["calendardate"] = {"lte": end_date}
    if columns:
        qopts["columns"] = columns

    if qopts:
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    return quandl_object.df


def get_price_data(
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
    datatable_code = "SHARADAR/SEP"
    filters = dict()
    # filters["ticker"] = ticker
    filters["paginate"] = True
    filters["ticker"] = ticker
    if start_date and end_date:
        filters["date"] = {"gte": start_date, "lte": end_date}
    elif start_date:
        filters["date"] = {"gte": start_date}
    elif end_date:
        filters["date"] = {"lte": end_date}
    if columns:
        qopts = {"columns": columns}
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "prices", datatable_code, filters)
    quandl_object.load()
    df = (
        quandl_object.df.sort_values(by=["ticker", "date"], ascending=True)
        .reset_index()
        .drop("index", axis=1)
    )
    return df


def get_company_actions(
    ticker: list,
    start_date: str = None,
    end_date: str = None,
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl company actions data through API

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
    datatable_code = "SHARADAR/ACTIONS"
    filters["ticker"] = ticker

    qopts = dict()
    if start_date and end_date:
        filters["date"] = {"gte": start_date, "lte": end_date}
    elif start_date:
        filters["date"] = {"gte": start_date}
    elif end_date:
        filters["date"] = {"lte": end_date}
    if columns:
        qopts["columns"] = columns

    if qopts:
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    return quandl_object.df


def get_company_estimated_earnings(
    ticker: list,
    report_period: str = "QUARTER",
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl company holders data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    report_period: str, optional
        report period, either 'QUARTER' or 'YEAR'
    columns: list, optional
        subset of columns to pull from table

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    api_key = "MxE6oNePp886npLJ2CGs"
    datatable_code = "ZACKS/EE"
    filters["ticker"] = ticker

    qopts = dict()
    if columns:
        qopts["columns"] = columns

    if qopts:
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    df = quandl_object.df
    if report_period == "QUARTER":
        df = df[df["per_type"] == "Q"]
    else:
        df = df[df["per_type"] == "A"]
    return df


def get_company_estimated_sales(
    ticker: list,
    report_period: str = "QUARTER",
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl company holders data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    report_period: str, optional
        report period, either 'QUARTER' or 'YEAR'
    columns: list, optional
        subset of columns to pull from table

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    api_key = "MxE6oNePp886npLJ2CGs"
    datatable_code = "ZACKS/SE"
    filters["ticker"] = ticker

    qopts = dict()
    if columns:
        qopts["columns"] = columns

    if qopts:
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    df = quandl_object.df
    if report_period == "QUARTER":
        df = df[df["per_type"] == "Q"]
    else:
        df = df[df["per_type"] == "A"]
    return df


def get_company_holders(
    ticker: list,
    columns: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl company holders data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API
    columns: list, optional
        subset of columns to pull from table

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    filters = dict()
    api_key = "MxE6oNePp886npLJ2CGs"
    datatable_code = "ZACKS/IHM"
    filters["ticker"] = ticker

    qopts = dict()
    if columns:
        qopts["columns"] = columns

    if qopts:
        filters["qopts"] = qopts

    quandl_object = quandl.Quandl(api_key, "fundamental", datatable_code, filters)
    quandl_object.load()
    return quandl_object.df


def get_market_data(
    start_date: str = None,
    end_date: str = None,
    columns: list = [
        "MULTPL/SP500_PSR_QUARTER",
        "MULTPL/SP500_PBV_RATIO_QUARTER",
        "MULTPL/SP500_PE_RATIO_MONTH",
    ],
) -> pd.DataFrame:
    """
    For a specified list of multiples, load Quandl market data through API

    Parameters
    ----------
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
    datatable_code = columns
    filters = dict()
    if start_date:
        filters["start_date"] = start_date
    if end_date:
        filters["end_date"] = end_date

    quandl_object = quandl.Quandl(api_key, "market", datatable_code, filters)
    quandl_object.load()
    return quandl_object.df
