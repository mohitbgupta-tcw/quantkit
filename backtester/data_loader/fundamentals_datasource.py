import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np
from copy import deepcopy
import datetime


class FundamentalsDataSource(ds.DataSources):
    """
    Provide fundamental information on company level

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        ticker: str
            ticker of company
        date: datetime.date
            date
        date_key: datetime.date
            release data
        fundamental kpi's: float
            various fundamental kpi's
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.tickers = dict()
        self.current_loc = 0
        self.fundamentals = dict()

    def load(self, start_date: str = None, end_date: str = None, **kwargs) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        start_date, optional: str, optional
            start date to pull from API
        end_date: str, optional
            end date to pull from API
        """
        logging.log(f"Loading Fundamental Data")
        ticker = (
            ", ".join(f"'{pf}'" for pf in self.params["filters"]["ticker"])
            if self.params["filters"]["ticker"]
            else "''"
        )
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        start_query = f"""AND "calendardate" >= '{start_date}'""" if start_date else ""
        end_query = f"""AND "calendardate" <= '{end_date}'""" if end_date else ""
        query = f"""
        SELECT * 
        FROM {from_table}
        WHERE "ticker" in ({ticker})
        {start_query}
        {end_query}
        """

        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - rename calendardate to date
        - order by ticker and date
        - calculate release date as date + 3 months
        """
        self.datasource.df = self.datasource.df.rename(columns={"calendardate": "date"})
        self.datasource.df["date"] = pd.to_datetime(self.datasource.df["date"])
        self.datasource.df["datekey"] = pd.to_datetime(self.datasource.df["datekey"])
        self.datasource.df = self.datasource.df.sort_values(
            by=["date", "ticker"], ascending=True, ignore_index=True
        )

        if self.params.get("overwrite_parent"):
            for sub_share_ticker, parent_ticker in self.params[
                "overwrite_parent"
            ].items():
                df_add = self.datasource.df[
                    self.datasource.df["ticker"] == parent_ticker
                ]
                df_add["ticker"] = sub_share_ticker
                self.datasource.df = pd.concat([self.datasource.df, df_add])

    def iter(self, tickers_ordered: list) -> None:
        """
        - Sanity check to see if all tickers have data
        - Attach fundamental information to dict
        - Save Fundamental Dates

        Parameters
        ----------
        tickers_ordered: list
            list of ordered tickers
        """
        # check if all securities have data
        fund_data = list(self.datasource.df["ticker"].unique())
        no_data = list(set(tickers_ordered) - set(fund_data))
        if no_data:
            raise KeyError(f"The following identifiers were not recognized: {no_data}")

        grouped = self.df.groupby("ticker")
        for c, fundamental_information in grouped:
            self.tickers[c] = fundamental_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_fundamentals = pd.DataFrame(columns=self.df.columns)
        self.tickers[np.nan] = deepcopy(empty_fundamentals)

        # fundamental dates -> date + 3 months
        self.dates = list(self.datasource.df["datekey"].sort_values().unique())

        # initialize kpi's
        funds = [
            "marketcap",
            "divyield",
            "roe",
            "fcfps",
            "pe",
            "ps",
            "pb",
            "roic",
            "ebit",
            "ev",
        ]
        for fund in funds:
            self.fundamentals[fund] = (
                self.datasource.df.pivot(
                    index="datekey", columns="ticker", values=fund
                )[tickers_ordered]
                .ffill()
                .to_numpy()
            )

    def outgoing_row(self, date: datetime.date) -> dict:
        """
        Return current market caps universe

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        dict
            current fundamentals of universe
        """
        while (
            self.current_loc < len(self.dates) - 1
            and date >= self.dates[self.current_loc + 1]
        ):
            self.current_loc += 1
        return_dict = dict()
        for fund in self.fundamentals:
            return_dict[fund] = self.fundamentals[fund][self.current_loc]
        return return_dict

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df

    def is_valid(self, date: datetime.date) -> bool:
        """
        check if inputs are valid

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return date >= self.dates[0]
