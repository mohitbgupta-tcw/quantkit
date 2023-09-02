import quantkit.data_sources.data_sources as ds
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
        Client_ID: str
            Security ISIN
        TRAILING_12M_R&D_%_SALES: float
            trailing 12 month Research and Development %
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Bloomberg Data")
        self.datasource.load()
        self.transform_df()

    def transform_df(self) -> None:
        """
        None
        """
        pass

    def iter(self, companies: dict) -> None:
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

        for c, comp_store in companies.items():
            if not hasattr(comp_store, "bloomberg_information"):
                comp_store.bloomberg_information = deepcopy(empty_bloomberg)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
