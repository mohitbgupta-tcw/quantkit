import quantkit.data_sources.data_sources as ds
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
        Transition_Target: str
            transition target
        Transition_Revenue: str
            transition revenue
        Transition_Watch_Target: str
            transition watch target
        Transition_Watch_Revenue: str
            transition watch revenue
        Acronym: str
            acronym for target
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Transition Mapping Data")
        self.datasource.load()
        self.transform_df()

    def transform_df(self) -> None:
        """
        - Title BCLASS (REITs -> Reits)
        """
        self.datasource.df["BCLASS_LEVEL4"] = self.datasource.df[
            "BCLASS_LEVEL4"
        ].str.title()

    def iter(self, gics: dict, bclass: dict) -> None:
        """
        For each Sub-Sector, assign transition targets and transition revenue

        Revenue_10	>10% Climate Revenue
        Revenue_20	>20% Climate Revenue
        Revenue_30	>30% Climate Revenue
        Revenue_40	>40% Climate Revenue
        Revenue_50	>50% Climate Revenue
        Revenue_60	>60% Climate Revenue
        Target_A	Approved SBTi
        Target_AA	Approved SBTi or Ambitious Target
        Target_AAC	Approved/Committed SBTi or Ambitious Target
        Target_AACN	Approved/Committed SBTi or Ambitious Target or Non-Ambitious Target
        Target_AC	Approved/Committed SBTi
        Target_CA	Committed SBTi or Ambitious Target
        Target_CN	Committed SBTi or Non-Ambitious Target
        Target_N	Non-Ambitious Target
        Target_NRev	Non-Ambitious Target AND >0% Climate Revenue

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
