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
        df = fred_object.df
        df = df.rename(columns={"realtime_start": "publish_date", "date": "date_from", "value": ticker})
        df = df.sort_values(by=["publish_date", "date_from"], ascending=True)

        first_publish_date = df["publish_date"].min()
        fred_revised_data = df[df["publish_date"] == first_publish_date][:-1].drop(["id"], axis=1)
        fred_revised_data["publish_date"] = fred_revised_data["date_from"] + pd.DateOffset(months=1)
        fred_revised_data = fred_revised_data.dropna(subset=ticker)
        fred_revised_data[ticker] = fred_revised_data[ticker].astype(float)
        for avg in averages:
            fred_revised_data[f"{ticker}_{avg}MA"] = fred_revised_data[ticker].rolling(avg).mean()
        for chg in changes:
            fred_revised_data[f"{ticker}_{chg}Change"] = (
                fred_revised_data[ticker].pct_change(chg)
            )
        for diff in differences:
            fred_revised_data[f"{ticker}_{diff}Diff"] = (
                fred_revised_data[ticker].diff(diff)
            )

        df["Revision"] = np.where(
            (df["publish_date"] - df["date_from"]) / pd.Timedelta("1D") > 90, True, False
        )

        for date in df[df["Revision"] == False]["publish_date"].unique():
            date_df = pd.DataFrame()
            row_df = df[df["publish_date"] <= date]
            row_df = row_df.sort_values(
                ["date_from", "publish_date"], ascending=True
            ).drop_duplicates("date_from", keep="last")
            date_df["publish_date"] = [date]
            date_df["date_from"] = row_df.tail(1)["date_from"].values
            date_df[ticker] = row_df.tail(1)[ticker].values
            for avg in averages:
                date_df[f"{ticker}_{avg}MA"] = row_df.tail(avg)[ticker].mean()
            for chg in changes:
                date_df[f"{ticker}_{chg}Change"] = (
                    row_df[ticker].pct_change(chg).tail(1).values
                )
            for diff in differences:
                date_df[f"{ticker}_{diff}Diff"] = (
                    row_df[ticker].diff(diff).tail(1).values
                )
            fred_revised_data = pd.concat([fred_revised_data, date_df])
        return fred_revised_data
    return fred_object.df
