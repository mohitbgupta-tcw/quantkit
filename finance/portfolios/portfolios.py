import pandas as pd


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

    def add_holdings(self, holdings_df: pd.DataFrame):
        """
        Safe historical holdings of portfolio with date, weight, oas, market value as
        dataframe in self.holdings_df.

        Parameters
        ----------
        holdings_df: pd.DataFrame

        """
        self.holdings_df = holdings_df
        return
