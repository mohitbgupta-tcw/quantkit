import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


class TransitionDataSource(ds.DataSources):
    """
    Provide information for transition targets and revenue per industry

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        GICS_SUB_IND: str
            GICS industry
        BCLASS_LEVEL4: str
            BClass industry
        ENABLER_IMPROVER: str
            Company GICS or BCLASS falls into Enabler or Improver category
        Transition_Revenue_Enabler: str
            transition revenue for enabler
        Transition_Revenue_Improver: str
            transition revenue for improver
        Transition_Target: str
            transition target
        DECARB: str
            decarbonization target
        CAPEX: str
            capex target
        Acronym: str
            acronym for target
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Transition Mapping Data")

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

    def iter(self, gics: dict, bclass: dict) -> None:
        """
        For each Sub-Sector, assign transition targets and transition revenue, capex, decarb and
        enabler/improver

        Parameters
        ----------
        gics: dict
            dictionary of all gics sub industries
        bclass: dict
            dictionary of all bclass sub industries
        """
        for index, row in self.df.iterrows():
            gics_sub = row["GICS_SUB_IND"]
            bclass4 = row["BCLASS_LEVEL4"]

            if not pd.isna(gics_sub):
                gics[gics_sub].add_transition(row.to_dict())
            if not pd.isna(bclass4):
                bclass[bclass4].add_transition(row.to_dict())

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
