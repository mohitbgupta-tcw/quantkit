import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging


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

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Transition Mapping Data")
        self.datasource.load()
        self.transform_df()
        return

    def transform_df(self):
        """
        None
        """
        self.datasource.df["BCLASS_LEVEL4"] = self.datasource.df[
            "BCLASS_LEVEL4"
        ].str.title()
        return

    @property
    def df(self):
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
