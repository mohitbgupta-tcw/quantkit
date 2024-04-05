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

    def __init__(self, type: str, filters: dict, **kwargs) -> None:
        self.type = type
        self.filters_prices = {
            key: value
            for key, value in filters.items()
            if key in ["start_date", "end_date", "tickers"]
        }
        self.filters_fundamental = {
            key: value for key, value in filters.items() if key in ["flds", "tickers"]
        }
        self.quarters = ["Q1", "Q2", "Q3", "Q4"]
        self.years = filters.get("years", None)
        self.report_period = filters.get("report_period", "ANNUAL")

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
        self.df = blp.bdh(
            flds=flds,
            **self.filters_prices,
        )

    def load_fundamentals(self) -> None:
        """
        Load fundamental data from Bloomberg API using BDP function and save as pd.DataFrame in self.df
        """
        self.df = pd.DataFrame()
        if self.report_period == "ANNUAL":
            for year in self.years:
                annual_df = blp.bdp(EQY_FUND_YEAR=year, **self.filters_fundamental)
                self.df = pd.concat([self.df, annual_df])
        elif self.report_period == "QUARTER":
            for year in self.years:
                for quarter in self.quarters:
                    quarter_df = blp.bdp(
                        EQY_FUND_YEAR=year, FUND_PER=quarter, **self.filters_fundamental
                    )
                    self.df = pd.concat([self.df, quarter_df])
