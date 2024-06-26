import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.risk_framework.core.categories as categories
import pandas as pd


class CategoryDataSource(ds.DataSources):
    """
    Provide indicators and flagging thresholds for each category in Materialty Map

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Analyst: str
            Analyst covering sector
        Sector: str
            Analyst category
        Sub-Sector: str
            Sub-Category
        Risk Category: str
            'ESRM' for normal categories, 'Governance' for region specific categories
        Indicator Field Name: str
            MSCI column attached to category
        Operator: str
            operator ('=', '<', '>') indicating if company value for indicator should be
            equal, below or above threshold
        Flag_Threshold: float
            threshold for indicator. If condition is true for company, flag this indicator
        Source: str
            MSCI
        Factor Name:
            description of indicator
        Threshold Columns: int
            Flag thresholds for Sector
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.categories = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Category Data")

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

    def iter(self) -> None:
        """
        For each category save indicator fields and EM and DM flag scorings in category object
        """

        for cat in self.df["Sub-Sector"].unique():
            cat_store = categories.Category(cat)

            # save indicator fields, operator, thresholds
            df_ss = self.df[self.df["Sub-Sector"] == cat]
            cat_store.add_esrm_df(df_ss)

            # save flag scorings for EM and DM
            df_ = df_ss.drop_duplicates(subset="Sub-Sector")
            dm = list(
                df_[
                    [
                        "Well-Performing",
                        "Above Average",
                        "Average",
                        "Below Average",
                        "Concerning",
                    ]
                ].values.flatten()
            )
            em = list(
                df_[
                    [
                        "Well-Performing-EM",
                        "Above Average-EM",
                        "Average-EM",
                        "Below Average-EM",
                        "Concerning-EM",
                    ]
                ].values.flatten()
            )
            cat_store.add_DM_flags(dm)
            cat_store.add_EM_flags(em)

            self.categories[cat] = cat_store

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
