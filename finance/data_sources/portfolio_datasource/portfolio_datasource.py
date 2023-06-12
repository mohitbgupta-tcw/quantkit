import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging


class PortfolioDataSource(ds.DataSources):
    """
    Provide portfolio data

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        As Of Date: datetime.date
            date of portfolio
        Portfolio: str
            portfolio id
        Portfolio name: str
            portfolio name
        ESG Collateral Type: str
            esg collateral type
        ISIN: str
            isin of security
        Issuer ESG: str
            is issuer ESG?
        Labeled ESG Type: str
            security esg type
        ISSUER_Name: str
            name of issuer
        TCW ESG: str
            ESG label of TCW
        Ticker Cd: str
            ticker of issuer
        Sector Level 1: str
            sector 1 of issuer
        Sector Level 2: str
            sector 2 of issuer
        BCLASS_level4: str
            BClass of issuer
        Portfolio_Weight: float
            weight of security in portfolio
        Base Mkt Value: float
            market value of position in portfolio
        Rating Raw MSCI: str
            MSCI rating of issuer
        OAS: float
            OAS
    """

    def __init__(self, params: dict):
        logging.log("Loading Portfolio Data")
        super().__init__(params)

    def transform_df(self):
        """
        - replace NA's in ISIN with 'NoISIN'
        - replace NA's in BCLASS_Level4 with 'Unassigned BCLASS'
        - change first letter of each word to upper, else lower case
        """
        self.datasource.df["ISIN"].fillna("NoISIN", inplace=True)
        self.datasource.df["ISIN"].replace("--", "NoISIN", inplace=True)
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].fillna("Unassigned BCLASS")
        self.datasource.df["BCLASS_Level4"].replace(
            "N/A", "Unassigned BCLASS", inplace=True
        )
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].str.title()
        self.datasource.df["BCLASS_Level4"].replace(
            "Unassigned Bclass", "Unassigned BCLASS", inplace=True
        )
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
