import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


class AdjustmentDataSource(ds.DataSources):
    """
    Provide analyst adjustment to specific ISIN's

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Analyst: str
            Analyst covering sector
        ISSUER_NAME: str
            company name
        ISIN: str
            MSCI Issuer ID
        Thematic Type: str
            Risk, People, Transition, Planet
        Category: str
            theme
        Adjustment: str
            analyst adjustment
        Comments: str
            analyst comment
        Last Updated: datetime.date
            last updated date
        Head of Research Clearance: str
            head of research cleared adjustment
        Date: datetime.date
            date
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.msci_ids = dict()
        self.security_isins = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Adjustment Data")
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        Sort datasource descending by 'Adjustment'
        """
        self.datasource.df = self.datasource.df.sort_values(
            by=["Adjustment"], ascending=False
        )

    def iter(self) -> None:
        """
        Attach analyst adjustment information to dicts
        """
        for index, row in self.df.iterrows():
            isin = row["ISIN"]

            adjustment_information = row.to_dict()
            if isin[:3] == "IID":
                if isin in self.msci_ids:
                    self.msci_ids[isin].append(adjustment_information)
                else:
                    self.msci_ids[isin] = [adjustment_information]
            else:
                if isin in self.security_isins:
                    self.security_isins[isin].append(adjustment_information)
                else:
                    self.security_isins[isin] = [adjustment_information]

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
