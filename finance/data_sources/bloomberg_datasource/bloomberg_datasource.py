import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
from copy import deepcopy


class BloombergDataSource(ds.DataSources):
    """
    Provide information on company level from Bloomberg

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

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Bloomberg Data")
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
        Attach bloomberg information to company objects

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        """
        # only iterate over companies we hold in the portfolios
        for index, row in self.df[
            self.df["Client_ID"].isin(companies.keys())
        ].iterrows():
            isin = row["Client_ID"]

            bloomberg_information = row.to_dict()
            companies[isin].bloomberg_information = bloomberg_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_bloomberg = pd.Series(np.nan, index=self.df.columns).to_dict()

        for c in companies:
            if not hasattr(companies[c], "bloomberg_information"):
                companies[c].bloomberg_information = deepcopy(empty_bloomberg)
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
