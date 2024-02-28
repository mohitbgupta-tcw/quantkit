import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
from copy import deepcopy


class PricesDataSource(ds.DataSources):
    """
    Provide price information on company level

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame:
        ticker: str
            ticker of company
        date: datetime.date
            date
        open: float
            open of date
        high: float
            high of date
        low: float
            low of date
        close: float
            close of date
        volume: float
            volume of date
        closeadj: float
            adjusted close (for dividends, splits, etc)
        closeundadj: float
            unadjusted close
        lastupdated: datetime.date
            date of last update
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.tickers = dict()

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
        logging.log(f"Loading Price Data")
        ticker = (
            ", ".join(f"'{pf}'" for pf in self.params["filters"]["ticker"])
            if self.params["filters"]["ticker"]
            else "''"
        )
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        start_query = f"""AND "date" >= '{start_date}'""" if start_date else ""
        end_query = f"""AND "date" <= '{end_date}'""" if end_date else ""
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
        order by ticker and date
        """
        if self.params["source"] == 9:
            self.datasource.df.columns = self.datasource.df.columns.droplevel(level=1)
            self.datasource.df.index = self.datasource.df.index.rename("date")
            self.datasource.df = self.datasource.df.reset_index()
            self.datasource.df = self.datasource.df.melt(
                id_vars=["date"], var_name="ticker", value_name="closeadj"
            )
            self.datasource.df = self.datasource.df.dropna()

        self.datasource.df["date"] = pd.to_datetime(self.datasource.df["date"])
        self.datasource.df = self.datasource.df.sort_values(
            by=["date", "ticker"], ascending=True, ignore_index=True
        )

    def iter(self, tickers_ordered: list) -> None:
        """
        - Create price DataFrame from closeadj
        - Create returns DataFrame from price data
        - Attach price information to dict
        - Sanity check to see if all tickers have data

        Parameters
        ----------
        tickers_ordered: list
            list of ordered tickers
        """
        # check if all securities have data
        price_cols = list(self.datasource.df["ticker"].unique())
        no_data = list(set(tickers_ordered) - set(price_cols))
        if no_data:
            raise KeyError(f"The following identifiers were not recognized: {no_data}")

        self.price_data = self.datasource.df.pivot(
            index="date", columns="ticker", values="closeadj"
        )
        self.return_data = self.price_data.pct_change(1)[tickers_ordered]

        grouped = self.df.groupby("ticker")
        for c, price_information in grouped:
            self.tickers[c] = price_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_prices = pd.DataFrame(columns=self.df.columns)
        self.tickers[np.nan] = deepcopy(empty_prices)

        # rebalance dates -> last trading day of month
        self.rebalance_dates = list(
            self.return_data.groupby(
                pd.Grouper(
                    freq=mapping_configs.pandas_translation[self.params["rebalance"]]
                )
            )
            .tail(1)
            .index
        )

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
