import pandas as pd
import datetime
import quantkit.finance.sectors.sectors as sectors


class PortfolioStore(object):
    """
    Portfolio Object. Stores information such as:
        - portfolio id
        - portfolio name
        - holdings (holds security object as well as holding measure (weight, OAS))
        - holdings dataframe

    Parameters
    ----------
    pf: str
        portfolio id, unique identifier for portfolio/ index
    name: str, optional
        portfolio name
    """

    def __init__(self, pf: str, name: str = None):
        self.id = pf
        self.name = name
        self.holdings = dict()

    def add_holdings(self, holdings_df: pd.DataFrame) -> None:
        """
        Safe historical holdings of portfolio with date, weight, oas, market value as
        dataframe in self.holdings_df.

        Parameters
        ----------
        holdings_df: pd.DataFrame

        """
        self.holdings_df = holdings_df

    def add_sector(self, sector: sectors.Sector) -> None:
        """
        Attach Sector to portfolio

        Parameters
        ----------
        sector: sectors.Sector
        """
        self.Sector = sector

    def add_as_of_date(self, as_of_date: datetime.date) -> None:
        """
        Attach As Of Date to portfolio

        Parameters
        ----------
        as_of_date: datetime.date
            as of date of portfolio
        """
        self.as_of_date = as_of_date
