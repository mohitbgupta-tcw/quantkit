import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


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

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.parent_issuers = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Parent Issuer Data")
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        None
        """
        pass

    def iter(self) -> None:
        """
        Manually add parent issuer for selected securities
        """
        for index, row in self.df.iterrows():
            self.parent_issuers[row["SECURITY_ISIN"]] = {
                "MSCI_ISSUERID": row["PARENT_ULTIMATE_ISSUERID"]
            }

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
