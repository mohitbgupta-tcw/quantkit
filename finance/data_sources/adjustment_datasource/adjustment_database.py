import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


class AdjustmentDataSource(ds.DataSources):
    """
    Provide analyst adjustment to specific ISIN's

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Analyst: str
            Analyst covering sector
        ISSUER_NAME: str
            company name
        ISIN: str
            MSCI Issuer ID
        Thematic Type: str
            Risk, People, Transition, Planet
        Category: str
            theme
        Adjustment: str
            analyst adjustment
        Comments: str
            analyst comment
        Last Updated: datetime.date
            last updated date
        Head of Research Clearance: str
            head of research cleared adjustment
        Date: datetime.date
            date
    """

    def __init__(self, params: dict, **kwargs):
        super().__init__(params, **kwargs)

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Adjustment Data")
        self.datasource.load()
        self.transform_df()

    def transform_df(self) -> None:
        """
        Sort datasource descending by 'Adjustment'
        """
        self.datasource.df = self.datasource.df.sort_values(
            by=["Adjustment"], ascending=False
        )

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
