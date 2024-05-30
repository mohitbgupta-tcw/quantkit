import pandas as pd
import datetime
from blp import blp


class Bloomberg(object):
    """
    Main class to load Bloomberg data using the blp package

    Parameters
    ----------
    fields: list
        list of fields to pull
    tickers: list
        list of tickers to pull
    start_date: datetime.date, optional
        start date
    end_date: datetime.date, optional
        end date
    filters: dict, optional
        dictionary of parameters for function call
    """

    def __init__(
        self,
        fields: list,
        tickers: list,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
        filters: dict = dict(),
        **kwargs,
    ) -> None:
        self.fields = fields
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date

        allowed_filters = ["ca_adj", "fill"]
        self.filters = {
            key: item for key, item in filters.items() if key in allowed_filters
        }

    def load(self, **kwargs) -> None:
        """
        Load data from Bloomberg API and save as pd.DataFrame in self.df
        """
        d = {"'": "", ":": " =", "{": "", "}": ""}
        bquery = blp.BlpQuery().start()

        data_str = ", ".join(self.fields)
        date_str = (
            f"dates=range({self.start_date},{self.end_date})" if self.start_date else ""
        )
        filter_str = (
            "," + "".join(d.get(l, l) for l in str(self.filters))
            if self.filters
            else ""
        )
        filters = "(" + date_str + filter_str + ")" if (filter_str or date_str) else ""
        query = f"get({data_str} {filters}) for({self.tickers})"

        self.df = bquery.bql(expression=query)
