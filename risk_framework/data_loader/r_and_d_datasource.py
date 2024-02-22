import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
from copy import deepcopy


class RandDDataSource(ds.DataSources):
    """
    Provide  Research & Development information on company level from Bloomberg

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
        self.research_development = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading R&D Data")

        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        Drop Duplicates of Bloomberg ID
        """
        self.datasource.df = self.datasource.df.drop_duplicates(subset=["BBG_ID"])

    def iter(self) -> None:
        """
        Attach R&D information to dict
        """
        for index, row in self.df.iterrows():
            bbg_id = row["BBG_ID"]

            bloomberg_information = row.to_dict()
            self.research_development[bbg_id] = bloomberg_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_bloomberg = pd.Series(np.nan, index=self.df.columns).to_dict()
        self.research_development[np.nan] = deepcopy(empty_bloomberg)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
