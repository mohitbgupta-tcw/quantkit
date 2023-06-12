import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging


class ThemeDataSource(ds.DataSources):
    """
    Provide information for each theme

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Pillar: str
            general pillar (People or Planet)
        Acronym: str
            theme id
        Theme: str
            theme description
        ISS 1: str
            column names from SDG datasource relevant for theme
        ISS 2: str
            column names from SDG datasource relevant for theme
        MSCI Summary Category: str
            Summary category for subcategory column
        MSCI Subcategories: str
            column names from MSCI datasource relevant for theme
        ProductKeyAdd: str
            words linked with theme
    """

    def __init__(self, params: dict):
        logging.log("Loading Thematic Mapping Data")
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
