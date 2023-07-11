import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
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

    def __init__(self, params: dict):
        super().__init__(params)

    def load(self, ticker: list):
        """
        load data and transform dataframe

        Parameters
        ----------
        ticker: list
            list of all tickers in portfolios
        """
        logging.log("Loading Quandl Data")

        self.params["filters"]["ticker"] = list(set(ticker))

        self.datasource.load()
        self.transform_df()
        return

    def transform_df(self):
        """
        None
        """
        return

    def iter(self, companies: dict):
        """
        Attach quandl information to company objects

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        """

        # --> not every company has these information, so create empty df with NA's for those
        empty_quandl = pd.Series(np.nan, index=self.df.columns).to_dict()

        for c in companies:
            t = companies[c].msci_information["ISSUER_TICKER"]
            quandl_information = self.df[self.df["ticker"] == t]
            if not quandl_information.empty:
                companies[c].quandl_information = quandl_information.squeeze().to_dict()
            else:
                companies[c].quandl_information = deepcopy(empty_quandl)
        return

    @property
    def df(self):
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
