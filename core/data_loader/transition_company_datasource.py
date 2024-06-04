import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


class TransitionCompanyDataSource(ds.DataSources):
    """
    Provide Information for Transition names which are Company Specific

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Name: str
            Company Name
        ISSUER ISIN: str
            Company ISIN
        ENABLER_IMPROVER: str
            Company GICS or BCLASS falls into Enabler or Improver category
        ACRONYM: str
            acronym for target
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.transition_mapping = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Transition Company Mapping Data")

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

    def iter(self, issuer_dict: dict) -> None:
        """
        Attach transition mapping information to dict

        Parameters
        ----------
        issuer_dict: dict
            dictionary of issuers
        """
        for index, row in self.df.iterrows():
            msci_id = row["MSCI ISSUERID"]

            msci_information = row.to_dict()
            self.transition_mapping[msci_id] = msci_information

        for iss, issuer_store in issuer_dict.items():
            issuer_store.attach_transition_info(self.transition_mapping)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
