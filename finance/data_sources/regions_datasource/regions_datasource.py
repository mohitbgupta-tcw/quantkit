import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import numpy as np


class RegionsDataSource(ds.DataSources):
    """
    Provide information about country and region data

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Country: str
            country name
        ISO3: str
            iso3 code of country
        ISO2: str
            iso2 code of country
        Region: str
            market of country (developed, emerging, EU, Japan)
        Region_EM: str
            country emerging or developed
        Region_Theme: str
            more granular classification of region into EM, DM, EU, Japan
        Sovereign_Score: int
            sovereign score
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Regions Data")
        self.datasource.load()
        self.transform_df()
        return

    def transform_df(self):
        """
        - Replace nan with NA (ISO2 for Namibia is read is nan)
        - add na row (for out of Scope companies without region)
        """
        self.df["ISO2"] = self.df["ISO2"].fillna("NA")
        self.df.loc[-1] = [np.nan, np.nan, np.nan, "DM", "DM", "DM", 0]
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
