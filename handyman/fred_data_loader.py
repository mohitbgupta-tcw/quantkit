import pandas as pd
import numpy as np
import quantkit.core.data_sources.fred as fred


def run_fred_api(
    ticker: str,
    revision: bool = True,
    realtime_start: str = "2017-01-01",
    averages: list = list(),
    changes: list = list(),
    differences: list = list(),
) -> pd.DataFrame:
    """
    Get economic datapoints through FRED API

    Parameters
    ----------
    ticker: str
        ticker from FRED
        (check available data here: https://fred.stlouisfed.org/)
    revision: bool, optional
        take revised series
    realtime_start: str, optional
        date for series to start at
    averages: list, optional
        calculate moving averages for window sizes in list
    changes: list, optional
        calculate percentage change for window size in list
    differences: list, optional
        calculate difference for window size in list

    Returns
    -------
    pd.DataFrame
        DataFrame of FRED information
    """
    api_key = "eb4f46c42913c92951a8c64d545598ca"

    filters = dict()
    filters["realtime_start"] = realtime_start

    fred_object = fred.FRED(
        api_key, tickers=[ticker], revision=revision, filters=filters
    )
    fred_object.load()
    if revision:
        fred_object.df = fred_object.df.rename(
            columns={"realtime_start": "publish_date"}
        )
        df = fred_object.df
        df = df.rename(columns={"realtime_start": "publish_date"})
        df = df.sort_values(by=["publish_date", "date"], ascending=True)
        df["Revision"] = np.where(
            (df["publish_date"] - df["date"]) / pd.Timedelta("1D") > 90, True, False
        )
        fred_revised_data = pd.DataFrame()
        for date in df[df["Revision"] == False]["publish_date"].unique():
            date_df = pd.DataFrame()
            row_df = df[df["publish_date"] <= date]
            row_df = row_df.sort_values(
                ["date", "publish_date"], ascending=True
            ).drop_duplicates("date", keep="last")
            date_df["publish_date"] = [date]
            date_df["date_from"] = row_df.tail(1)["date"].values
            date_df[ticker] = row_df.tail(1)["value"].values
            for avg in averages:
                date_df[f"{ticker}_{avg}MA"] = row_df.tail(avg)["value"].mean()
            for chg in changes:
                date_df[f"{ticker}_{chg}Change"] = (
                    row_df["value"].pct_change(chg).tail(1).values
                )
            for diff in differences:
                date_df[f"{ticker}_{diff}Diff"] = (
                    row_df["value"].diff(diff).tail(1).values
                )
            fred_revised_data = pd.concat([fred_revised_data, date_df])
        return fred_revised_data
    return fred_object.df
