import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
from copy import deepcopy


class ExclusionsDataSource(ds.DataSources):
    """
    Data Source for Exclusion data from MSCI

    Parameters
    ----------
    params: dict
        datasource specific parameters including source
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.exclusions = dict()
        self.table_name = params["table_name"] if "table_name" in params else ""

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Exclusions Data")
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - fill na
        """
        self.datasource.df = self.datasource.df.fillna(value=np.nan)

    def iter(self, issuer_dict: dict) -> None:
        """
        Attach exclusion information to dict

        Parameters
        ----------
        issuer_dict: dict
            dict of issuers
        """
        for index, row in self.df.iterrows():
            isin = row["ISSUERID"]

            exclusion_information = row.to_dict()
            self.exclusions[isin] = exclusion_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_exclusion = pd.Series(np.nan, index=self.df.columns).to_dict()
        self.exclusions["NoISSUERID"] = deepcopy(empty_exclusion)

        for iss, issuer_store in issuer_dict.items():
            issuer_store.attach_exclusion(self.exclusions)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
