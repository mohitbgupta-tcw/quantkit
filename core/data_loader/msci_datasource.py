import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
from copy import deepcopy


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
        self.msci = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading MSCI Data")
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        if self.params["historical"]:
            self.datasource.load_historical(query=query)
        else:
            self.datasource.load(query=query)
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

        if self.params.get("gics_overwrite"):
            for sec, trans in self.params["gics_overwrite"].items():
                for col, col_value in trans.items():
                    self.datasource.df.loc[
                        self.datasource.df["ISSUERID"] == sec, col
                    ] = col_value

    def iter(self) -> None:
        """
        Attach msci information to dict
        """
        for index, row in self.df.iterrows():
            msci_id = row["ISSUERID"]

            msci_information = row.to_dict()
            self.msci[msci_id] = msci_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_msci = pd.Series(np.nan, index=self.df.columns).to_dict()
        self.msci["NoISSUERID"] = deepcopy(empty_msci)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
