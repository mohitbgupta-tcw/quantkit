import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np


class MSCIDataSource(ds.DataSources):
    """
    Provide information on company level from MSCI

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

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading MSCI Data")
        if self.params["historical"]:
            self.datasource.load_historical()
        else:
            self.datasource.load()
        self.transform_df()

    def transform_df(self) -> None:
        """
        - fill GICS na's with 'Unassigned GICS'
        - translate None's to nan's
        - change values for columns in params["transformation"]
        """
        # fill GICS industry NA's with 'Unassigned GICS'
        self.datasource.df["GICS_SUB_IND"] = self.datasource.df["GICS_SUB_IND"].fillna(
            "Unassigned GICS"
        )

        # replace None by nan
        self.datasource.df = self.datasource.df.fillna(value=np.nan)

        # replace values in each column from params transformation file
        self.datasource.df = self.datasource.df.replace(self.params["transformation"])

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
