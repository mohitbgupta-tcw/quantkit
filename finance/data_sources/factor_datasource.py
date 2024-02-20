import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np
from copy import deepcopy
import datetime


class FactorsDataSource(ds.DataSources):
    """
    Provide Fama French 5 Factor-Model Factors
    See: https://statics.teams.cdn.office.net/evergreen-assets/safelinks/1/atp-safelinks.html

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Date: str
            date
        MKT-RF: float
            Market Return - Risk Free Rate
        SMB: float
            Small Stocks - Big Stocks
        HML: float
            High B/M Stocks - Low B/M Stocks
        RMW: float
            Stocks with Robust Profitability - Stocks with Weak Profitability
        CMA: float
            Conservative (Low Investment Firms) - Aggressive (High Investment Firms)
        RF: float
            Risk Free Rate
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.current_loc = 0

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log(f"Loading Factor Data")
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """

        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - Transform Date column into date format
        - Make Date index
        """
        self.datasource.df["Date"] = pd.to_datetime(
            self.datasource.df["Date"], format="%Y%m"
        )
        self.datasource.df.set_index("Date", inplace=True)

    def iter(self) -> None:
        """
        - Save Factor Dates
        """
        # fundamental dates -> date + 3 months
        self.dates = list(self.datasource.df.index.sort_values().unique())
        self.factor_matrix = self.datasource.df.to_numpy()

    def outgoing_row(self, date: datetime.date) -> np.ndarray:
        """
        Return current factors

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        np.array
            current factors
        """
        while (
            self.current_loc < len(self.dates) - 1
            and date >= self.dates[self.current_loc + 1]
        ):
            self.current_loc += 1
        return self.factor_matrix[self.current_loc]

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df

    def is_valid(self, date: datetime.date) -> bool:
        """
        check if inputs are valid

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return date >= self.dates[0]
