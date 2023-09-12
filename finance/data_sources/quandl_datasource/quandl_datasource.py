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

    def load(self, ticker: list) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        ticker: list
            list of all tickers in portfolios
        """
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        if self.params["type"] == "fundamental":
            t = "FUNDAMENTAL"
        else:
            t = "PRICE"

        logging.log(f"Loading Quandl {t} Data")
        self.params["filters"]["ticker"] = list(set(ticker))

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

    def iter(self, companies: dict) -> None:
        """
        Attach quandl information to company objects

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        """

        # --> not every company has these information, so create empty df with NA's for those
        empty_quandl = pd.Series(np.nan, index=self.df.columns)

        grouped = self.df.groupby("ticker")
        for c, quandl_information in grouped:
            if c in companies:
                if self.params["type"] == "fundamental":
                    companies[c].quandl_information = quandl_information
                elif self.params["type"] == "price":
                    companies[c].quandl_information_price = quandl_information

        for c, comp_store in companies.items():
            if self.params["type"] == "fundamental":
                if not hasattr(comp_store, "quandl_information"):
                    comp_store.quandl_information = deepcopy(empty_quandl)
                elif self.params["type"] == "price":
                    comp_store.quandl_information_price = deepcopy(empty_quandl)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
