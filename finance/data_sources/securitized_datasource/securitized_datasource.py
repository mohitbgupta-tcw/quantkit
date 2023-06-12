import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging


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

    def __init__(self, params: dict):
        logging.log("Loading Securitized Mapping")
        super().__init__(params)

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
