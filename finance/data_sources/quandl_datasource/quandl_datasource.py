import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np
from copy import deepcopy


class QuandlDataSource(ds.DataSources):
    """
    Provide information on company level from Quandl

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.quandl = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
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
        if self.params["type"] == "fundamental":
            t = "FUNDAMENTAL"
        else:
            t = "PRICE"

        logging.log(f"Loading Quandl {t} Data")
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        order by ticker and date
        """
        if "calendardate" in self.datasource.df.columns:
            self.datasource.df = self.datasource.df.rename(
                columns={"calendardate": "date"}
            )
        self.datasource.df["date"] = pd.to_datetime(self.datasource.df["date"])
        self.datasource.df = self.datasource.df.sort_values(
            by=["ticker", "date"], ascending=True, ignore_index=True
        )
        if self.params["type"] == "fundamental":
            self.datasource.df["release_date"] = (
                self.datasource.df["date"] + pd.DateOffset(months=3) + MonthEnd(0)
            )
            self.datasource.df["release_date"] = pd.to_datetime(
                self.datasource.df["release_date"]
            )
        self.datasource.df["date"] = pd.to_datetime(self.datasource.df["date"])

        if self.params.get("duplication"):
            for new_ticker, original_ticker in self.params["duplication"].items():
                df_add = self.datasource.df[
                    self.datasource.df["ticker"] == original_ticker
                ]
                df_add["ticker"] = new_ticker
                self.datasource.df = pd.concat([self.datasource.df, df_add])

    def iter(self) -> None:
        """
        Attach bloomberg information to dict
        """
        grouped = self.df.groupby("ticker")
        for c, quandl_information in grouped:
            self.quandl[c] = quandl_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_quandl = pd.DataFrame(columns=self.df.columns)
        self.quandl[np.nan] = deepcopy(empty_quandl)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
