import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.finance.themes.themes as themes


class ThemeDataSource(ds.DataSources):
    """
    Provide information for each theme

    Parameters
    ----------
    params: dict
        datasource specific parameters including source
    theme_calculations: dict
        theme calculation parameters

    Returns
    -------
    DataFrame
        Pillar: str
            general pillar (People or Planet)
        Acronym: str
            theme id
        Theme: str
            theme description
        ISS 1: str
            column names from SDG datasource relevant for theme
        ISS 2: str
            column names from SDG datasource relevant for theme
        MSCI Summary Category: str
            Summary category for subcategory column
        MSCI Subcategories: str
            column names from MSCI datasource relevant for theme
        ProductKeyAdd: str
            words linked with theme
    """

    def __init__(self, params: dict, theme_calculations: dict):
        super().__init__(params)
        self.theme_calculations = theme_calculations
        self.themes = dict()

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Thematic Mapping Data")
        self.datasource.load()
        self.transform_df()
        return

    def transform_df(self):
        """
        None
        """
        return

    def iter(self):
        """
        - create Theme objects for each theme
        - save object for each theme in self.themes
        - key is Acronym
        """
        df_ = self.df
        df_unique = df_.drop_duplicates(subset=["Acronym"])
        for index, row in df_unique.iterrows():
            theme = row["Acronym"]
            df_information = df_[df_["Acronym"] == theme]
            self.themes[theme] = themes.Theme(
                acronym=theme,
                name=row["Theme"],
                pillar=row["Pillar"],
                information_df=df_information,
                params=self.theme_calculations,
            )
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
