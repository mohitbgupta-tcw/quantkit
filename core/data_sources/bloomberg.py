import pandas as pd
from xbbg import blp


class Bloomberg(object):
    """
    Main class to load Bloomberg data using the xbbg package

    Parameters
    ----------
    type: str
        either "prices" or "fundamental"
    filters: dict
        dictionary of parameters for function call
    """

    def __init__(
        self, type: str, filters: dict, **kwargs
    ) -> None:
        self.type = type
        self.filters = filters


    def load(self, **kwargs) -> None:
        """
        Load data from Bloomberg API and save as pd.DataFrame in self.df
        """
        if self.type == "prices":
            self.load_prices()
        elif self.type == "fundamental":
            self.load_fundamentals()

    def load_prices(self) -> None:
        """
        Load price data from Bloomberg API using BDH function and save as pd.DataFrame in self.df
        """
        flds = ["PX_LAST"]
        df = blp.bdh(
            flds=flds,
            **self.filters,
        )
        df.columns = df.columns.droplevel(level=1)
        df.index = df.index.rename("date")
        df.index = pd.to_datetime(df.index)
        self.df = df

    def load_fundamentals(self) -> None:
        """
        """
        pass