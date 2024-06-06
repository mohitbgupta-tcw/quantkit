import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


class TickerParentIssuerSource(ds.DataSources):
    """
    Provide information about parent issuer for selected tickers

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        sub_share_ticker: str
            ticker of child
        parent_ticker: str
            ticker of issuer
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.parent_issuers = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Ticker Parent Issuer Data")
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        None
        """
        pass

    def tickers(self) -> list:
        """
        For specified securities, return a list of tickers of their parent

        Returns
        -------
        list
            list of tickers for parents
        """
        if self.parent_issuers:
            tickers = list(self.df["parent_ticker"].dropna().unique())
            return tickers
        return list()

    def iter(self) -> None:
        """
        Manually add parent issuer for selected securities
        """
        for index, row in self.df.iterrows():
            parent_ticker = row["parent_ticker"]
            sec = row["sub_share_ticker"]
            self.parent_issuers[sec] = parent_ticker

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
