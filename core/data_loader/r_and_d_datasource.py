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

    def iter(self, issuer_dict: dict) -> None:
        """
        Attach R&D information to dict

        Parameters
        ----------
        issuer_dict: dict
            dict of issuers
        """
        for index, row in self.df.iterrows():
            bbg_id = row["BBG_ID"]

            bloomberg_information = row.to_dict()
            self.research_development[bbg_id] = bloomberg_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_bloomberg = pd.Series(np.nan, index=self.df.columns).to_dict()
        self.research_development["NoISSUERID"] = deepcopy(empty_bloomberg)

        for iss, issuer_store in issuer_dict.items():
            bbg_id = (
                "NoISSUERID"
                if issuer_store.information["BBG_ISSUERID"] == "NoISSUERID"
                else float(issuer_store.information["BBG_ISSUERID"])
            )
            rud_information = deepcopy(
                self.research_development.get(
                    bbg_id, self.research_development["NoISSUERID"]
                )
            )

            parent_id = issuer_store.msci_information["PARENT_ULTIMATE_ISSUERID"]
            rud_information_parent = None
            if not pd.isna(parent_id):
                parent_store = issuer_dict.get(parent_id, None)
                if parent_store:
                    parent_bbg_id = (
                        "NoISSUERID"
                        if parent_store.information["BBG_ISSUERID"] == "NoISSUERID"
                        else float(parent_store.information["BBG_ISSUERID"])
                    )
                    rud_information_parent = deepcopy(
                        self.research_development.get(parent_bbg_id, None)
                    )

            issuer_store.attach_rud_information(rud_information, rud_information_parent)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
