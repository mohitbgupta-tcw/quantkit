import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import numpy as np


class ParentIssuerSource(ds.DataSources):
    """
    Provide information about parent issuer for selected securities

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        ISSUER_NAME: str
            issuer name
        SECURITY_ISIN: str
            security isin
        PARENT_ULTIMATE_ISSUERID: str
            msci id of ultimate parent
        ISIN: str
            parent isin
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Parent Issuer Data")
        self.datasource.load()
        self.transform_df()
        return

    def transform_df(self):
        """
        None
        """
        return
    
    def iter(self, companies: dict, securities: dict):
        """
        Manually add parent issuer for selected securities

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        securities: dict
            dictionary of all security objects
        """
        for index, row in self.df.iterrows():
            parent = row["ISIN"]
            sec = row["SECURITY_ISIN"]
            if parent in companies:
                if sec in securities:
                    companies[parent].add_security(
                        isin=sec, store=securities[sec]
                    )
                    securities[
                        sec
                    ].add_parent(companies[parent])
                if sec in companies:
                    del companies[sec]
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
