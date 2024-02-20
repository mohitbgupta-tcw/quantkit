import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import numpy as np
import pandas as pd
import quantkit.finance.regions.regions as regions


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

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.regions = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Regions Data")

        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - Replace nan with NA (ISO2 for Namibia is read is nan)
        - add na row (for out of Scope companies without region)
        """
        self.df["ISO2"] = self.df["ISO2"].fillna("NA")
        self.df.loc[-1] = [np.nan, np.nan, np.nan, "DM", "DM", "DM", 0]

    def iter(self) -> None:
        """
        - create Region objects for each region
        - save object for each region in self.regions
        - key is ISO2
        """
        for index, row in self.df.iterrows():
            r = row["ISO2"]
            self.regions[r] = regions.Region(r, row)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
