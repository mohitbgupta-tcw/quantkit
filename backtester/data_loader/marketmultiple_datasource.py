import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np
from copy import deepcopy
import datetime


class MarketMultipleDataSource(ds.DataSources):
    """
    Provide market information

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        date: datetime.date
            date
        multiple kpi's: float
            various multiple kpi's
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.current_loc = 0
        self.multiples = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log(f"Loading Market Multiples Data")
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """

        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - release date
        - Fill NaN's with prior values
        - order by date
        """
        self.datasource.df = self.datasource.df.fillna(method="ffill")
        self.datasource.df = self.datasource.df.sort_index(ascending=True)
        self.datasource.df = self.datasource.df.rename(
            columns={
                "MULTPL/SP500_PSR_QUARTER - Value": "SPX_PS",
                "MULTPL/SP500_PBV_RATIO_QUARTER - Value": "SPX_PB",
                "MULTPL/SP500_PE_RATIO_MONTH - Value": "SPX_PE",
            }
        )

    def iter(self) -> None:
        """
        Assign multiples to dictionary
        """
        # fundamental dates -> date + 3 months
        self.dates = list(self.datasource.df.index.unique())
        # initialize kpi's
        for kpi in self.datasource.df.columns:
            self.multiples[kpi] = self.datasource.df[kpi].to_numpy()

    def outgoing_row(self, date: datetime.date) -> dict:
        """
        Return current market caps universe

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        dict
            current fundamentals of universe
        """
        while (
            self.current_loc < len(self.dates) - 1
            and date >= self.dates[self.current_loc + 1]
        ):
            self.current_loc += 1
        return_dict = dict()
        for kpi in self.multiples:
            return_dict[kpi] = self.multiples[kpi][self.current_loc]
        return return_dict

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
