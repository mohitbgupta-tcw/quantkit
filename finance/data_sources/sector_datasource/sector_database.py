import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging


class SectorDataSource(ds.DataSources):
    """
    Sector per portfolio.
    Assign to either GICS (Equity fund) or BCLASS (Fixed Income fund)

    Parameter
    ---------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Portfolio: str
            portfolio id
        Sector_Code: str
            sector
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Sector Data")
        self.datasource.load()
        self.transform_df()
        return

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


class BClassDataSource(ds.DataSources):
    """
    Load BClass data
    Assign Bclass to industry, ESRM module etc.

    Parameter
    ---------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Analyst: str
            analyst name
        INDUSTRY_BCLASS_LEVEL3: str
            BClass level 3
        Industry: str
            industry
        BCLASS_Level4: str
            BClass level 4
        ESRM Module: str
            esrm module (analyst category)
        Tradition Risk Module: str
            transition risk of industry
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def load(self):
        """
        load data and transform dataframe
        """
        self.datasource.load()
        self.transform_df()
        return

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


class GICSDataSource(ds.DataSources):
    """
    Load GICS data
    Assign GICS to industry, ESRM module etc.

    Parameter
    ---------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Analyst: str
            analyst name
        SECTOR: str
            sector
        INDUSTRY_GROUP: str
            industry group
        Industry: str
            industry
        GICS_SUB_IND: str
            gics
        ESRM Module: str
            esrm module (analyst category)
        Tradition Risk Module: str
            transition risk of industry
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def load(self):
        """
        load data and transform dataframe
        """
        self.datasource.load()
        self.transform_df()
        return

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
