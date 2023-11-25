import quantkit.data_sources.data_sources as ds
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
        release_date: datetime.date
            date + 3 months
        fundamental kpi's: float
            various fundamental kpi's
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.tickers = dict()
        self.current_loc = 0

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log(f"Loading Fundamental Data")
        ticker = (
            ", ".join(f"'{pf}'" for pf in self.params["filters"]["ticker"])
            if self.params["filters"]["ticker"]
            else "''"
        )
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        WHERE "ticker" in ({ticker})
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
        self.datasource.df = self.datasource.df.sort_values(
            by=["date", "ticker"], ascending=True, ignore_index=True
        )
        self.datasource.df["release_date"] = (
            self.datasource.df["date"] + pd.DateOffset(months=3) + MonthEnd(0)
        )
        self.datasource.df["release_date"] = pd.to_datetime(
            self.datasource.df["release_date"]
        )

        if self.params.get("duplication"):
            for new_ticker, original_ticker in self.params["duplication"].items():
                df_add = self.datasource.df[
                    self.datasource.df["ticker"] == original_ticker
                ]
                df_add["ticker"] = new_ticker
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
        self.fundamental_dates = list(
            self.datasource.df["release_date"].sort_values().unique()
        )

        # initialize market caps
        self.marketcaps = self.datasource.df.pivot(
            index="release_date", columns="ticker", values="marketcap"
        )[tickers_ordered].to_numpy()

    def outgoing_row(self, date: datetime.date) -> np.array:
        """
        Return current market caps universe

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        np.array
            current market caps of universe
        """
        if date >= self.fundamental_dates[self.current_loc + 1]:
            self.current_loc += 1
        return self.marketcaps[self.current_loc]

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
