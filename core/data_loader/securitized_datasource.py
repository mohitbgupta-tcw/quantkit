import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


class SecuritizedDataSource(ds.DataSources):
    """
    Provide mapping for Securitized

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        G/S/S: str
            'Green', 'Social', 'Sustainable'
        ESG Collat Type: str
            esg collateral type
        Sustainability Theme - Primary: str
            primary theme
        Sustainability Theme - Secondary : str
            secondary theme
        Sclass_Level3: str
            share class level 3
        Primary: str
            primary theme
        Secondary: int
            seconday theme
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.securitized_mapping = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Securitized Mapping")
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

    def iter(self, securities: dict) -> None:
        """
        Iterate over securitized mapping and add to dictionary

        Parameters
        ----------
        securities: dict
            dict of securities
        """
        for index, row in self.df.iterrows():
            collat_type = row["ESG Collat Type"]
            self.securitized_mapping[collat_type] = row.to_dict()

        for sec, sec_store in securities.items():
            sec_store.add_collateral_type(self.securitized_mapping)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
