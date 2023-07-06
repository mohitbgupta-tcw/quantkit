import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


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

    @property
    def df(self):
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
