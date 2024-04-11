import quantkit.core.data_sources.bloomberg as bloomberg
import pandas as pd


def get_mapping_data(tickers: list) -> pd.DataFrame:
    """
    For a specified list of securities, load ID, ISIN, Country, GICS Industry and GICS Sector

    Parameters
    ----------
    ticker: list
        list of bloomberg tickers to run in API, p.e. ["AAPL US EQUITY", "MSFT US Equity"]

    Returns
    -------
    pd.DataFrame
        DataFrame of Bloomberg information
    """
    bloomberg_object = bloomberg.Bloomberg(
        fields=[
            "id_bb_company",
            "id_isin",
            "name",
            "cntry_of_domicile",
            "gics_sector_name",
            "gics_industry_name",
        ],
        tickers=tickers,
    )
    bloomberg_object.load()
    df = bloomberg_object.df
    df = df.pivot(columns="field", index="security", values="value")
    df = df[
        [
            "id_bb_company",
            "id_isin",
            "name",
            "cntry_of_domicile",
            "gics_sector_name",
            "gics_industry_name",
        ]
    ]
    return df


def get_price_data(
    tickers: list, start_date: str, end_date: str, ca_adj: str = "SPLITS"
) -> pd.DataFrame:
    """
    For a specified list of securities, load Bloomberg price data through API

    Parameters
    ----------
    ticker: list
        list of bloomberg tickers to run in API, p.e. ["AAPL US EQUITY", "MSFT US Equity"]
    start_date: str
        date in format "yyyy-mm-dd"
    end_date: str
        date in format "yyyy-mm-dd"
    ca_adj: str, optional
        corporate action adjustment, should be one of:
            - SPLITS: Adjusts data for stock splits only
            - FULL: Adjusts data for corporate actions that impact pricing and
                    number of shares outstanding, including splits, stock dividends,
                    spin-offs, rights offerings, etc
            - RAW: Leaves data unadjusted for corporate actions
    Returns
    -------
    pd.DataFrame
        DataFrame of Bloomberg information
    """
    filters = dict()
    filters["fillna"] = "NA"
    filters["ca_adj"] = ca_adj

    bloomberg_object = bloomberg.Bloomberg(
        fields=["PX_LAST"],
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        filters=filters,
    )
    bloomberg_object.load()

    df = bloomberg_object.df
    df = df[df["secondary_name"] == "DATE"]
    df = df.rename(
        columns={"security": "ticker", "value": "closeadj", "secondary_value": "date"}
    )
    df = df[["date", "ticker", "closeadj"]]
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.tz_localize(None)
    df = df.sort_values(["date", "ticker"], ascending=True)
    df = df.pivot(index="date", columns="ticker", values="closeadj")
    return df


def get_fundamental_data(
    tickers: list,
    fields: list,
    start_date: str,
    end_date: str,
    report_period: str = "LTM",
    fill: str = "NA",
    adjusted_list: list = list(),
) -> pd.DataFrame:
    """
    For a specified list of securities, load Bloomberg price data through API

    Parameters
    ----------
    ticker: list
        list of bloomberg tickers to run in API, p.e. ["AAPL US EQUITY", "MSFT US Equity"]
    fields: list
        list of fields from balance. financial sheet to pull
    start_date: str
        date in format "yyyy-mm-dd"
    end_date: str
        date in format "yyyy-mm-dd"
    report_period: str, optional
        report period, either 'LTM', 'Q', 'A'
    fill: str, optional
    adjusted_list: list, optional
        datapoints that take adjusted instead of reported data

    Returns
    -------
    pd.DataFrame
        DataFrame of Bloomberg information
    """
    filters = (
        f"fpt={report_period}, "
        + f"dates=range({start_date},{end_date}), "
        + f"fill={fill}"
    )
    fields = [
        f + "(" + filters + ", adj=Y" + ")"
        if f in adjusted_list
        else f + "(" + filters + ")"
        for f in fields
    ]

    bloomberg_object = bloomberg.Bloomberg(fields=fields, tickers=tickers)
    bloomberg_object.load()

    df = bloomberg_object.df

    df = df[df["secondary_name"] == "AS_OF_DATE"]
    df = df.rename(columns={"security": "ticker", "secondary_value": "date"})
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.tz_localize(None)
    df = df[["ticker", "field", "date", "value"]]
    df["field"] = df["field"].str.split("(").str[0]
    df = df.pivot(index=["date", "ticker"], columns="field", values="value")
    return df


def get_holdings_data(tickers: list, as_of_date: str = None) -> pd.DataFrame:
    """
    For a specified list of funds, load Bloomberg price data through API

    Parameters
    ----------
    ticker: list
        list of bloomberg tickers to run in API, p.e. ["AAPL US EQUITY", "MSFT US Equity"]
    as_of_date: str
        as of date in format "yyyy-mm-dd"

    Returns
    -------
    pd.DataFrame
        DataFrame of Bloomberg information
    """
    holdings_df = pd.DataFrame()
    as_of_date_str = ", dates=" + as_of_date if as_of_date else ""
    for ticker in tickers:
        bloomberg_object = bloomberg.Bloomberg(
            fields=["Weights"],
            tickers="holdings('" + ticker + "'" + as_of_date_str + ")",
        )
        bloomberg_object.load()
        df = bloomberg_object.df
        df["fund"] = ticker
        df["date"] = as_of_date if as_of_date else pd.to_datetime("today").normalize()
        holdings_df = pd.concat([holdings_df, df])

    holdings_df = holdings_df.rename(
        columns={"security": "ticker", "value": "Portfolio_Weight"}
    )
    holdings_df = holdings_df[["date", "fund", "ticker", "Portfolio_Weight"]]

    return holdings_df
