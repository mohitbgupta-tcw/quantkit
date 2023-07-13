import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.finance.sectors.sectors as sectors
import pandas as pd


class SectorDataSource(ds.DataSources):
    """
    Sector per portfolio.
    Assign to either GICS (Equity fund) or BCLASS (Fixed Income fund)

    Parameter
    ---------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        Portfolio: str
            portfolio id
        Sector_Code: str
            sector
    """

    def __init__(self, params: dict):
        super().__init__(params)
        self.sectors = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Sector Data")
        self.datasource.load()
        self.transform_df()

    def iter(self) -> None:
        """
        create Sector objects for GICS and BCLASS
        """
        for s in list(self.df["Sector_Code"].unique()):
            self.sectors[s] = sectors.Sector(s)

    def iter_portfolios(self, portfolios: dict) -> None:
        """
        Attach sector to each portfolio

        Parameters
        ----------
        portfolios: dict
            dictionary of all portfolios
        """
        df_ = self.df
        for index, row in df_[
            df_["Portfolio"].isin(list(portfolios.keys()))
        ].iterrows():
            pf = row["Portfolio"]
            portfolios[pf].add_sector(self.sectors[row["Sector_Code"]])

    def transform_df(self) -> None:
        """
        None
        """
        pass

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df


class BClassDataSource(ds.DataSources):
    """
    Load BClass data
    Assign Bclass to industry, ESRM module etc.

    Parameter
    ---------
    params: dict
        datasource specific parameters including source
    transition_params: dict
        transition paramaters

    Returns
    -------
    DataFrame
        Analyst: str
            analyst name
        INDUSTRY_BCLASS_LEVEL3: str
            BClass level 3
        Industry: str
            industry
        BCLASS_Level4: str
            BClass level 4
        ESRM Module: str
            esrm module (analyst category)
        Tradition Risk Module: str
            transition risk of industry
    """

    def __init__(self, params: dict, transition_params: dict):
        super().__init__(params)
        self.transition_params = transition_params
        self.bclass = dict()
        self.industries = dict()

        self.industries["Unassigned BCLASS High"] = sectors.Industry(
            "Unassigned BCLASS High", transition_risk="High", **transition_params
        )
        self.industries["Unassigned BCLASS Low"] = sectors.Industry(
            "Unassigned BCLASS Low", transition_risk="Low", **transition_params
        )

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        self.datasource.load()
        self.transform_df()

    def transform_df(self) -> None:
        """
        None
        """
        pass

    def iter(self) -> None:
        """
        Create Sub Sector and Industry objects for specific sector
        Save objects in sub-sector and industry attributes.
        """
        # create Sub-Sector and Industry objects
        for index, row in self.df.iterrows():
            # create Sub Sector object
            sub_sector = row["BCLASS_Level4"]
            self.bclass[sub_sector] = sectors.BClass(
                class_name=sub_sector, row_information=row
            )
            ss_object = self.bclass[sub_sector]

            # create Industry object if not already available
            industry = row["Industry"]
            self.industries[industry] = self.industries.get(
                industry,
                sectors.Industry(
                    industry,
                    transition_risk=row["Transition Risk Module"],
                    **self.transition_params
                ),
            )

            # assign Sub Sector object to Industry and vice verse
            self.industries[industry].add_sub_sector(ss_object)
            ss_object.add_industry(self.industries[industry])

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df


class GICSDataSource(ds.DataSources):
    """
    Load GICS data
    Assign GICS to industry, ESRM module etc.

    Parameter
    ---------
    params: dict
        datasource specific parameters including source
    transition_params: dict
        transition paramaters

    Returns
    -------
    DataFrame
        Analyst: str
            analyst name
        SECTOR: str
            sector
        INDUSTRY_GROUP: str
            industry group
        Industry: str
            industry
        GICS_SUB_IND: str
            gics
        ESRM Module: str
            esrm module (analyst category)
        Tradition Risk Module: str
            transition risk of industry
    """

    def __init__(self, params: dict, transition_params: dict):
        super().__init__(params)
        self.transition_params = transition_params
        self.gics = dict()
        self.industries = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        self.datasource.load()
        self.transform_df()

    def transform_df(self) -> None:
        """
        None
        """
        pass

    def iter(self) -> None:
        """
        Create Sub Sector and Industry objects for specific sector
        Save objects in sub-sector and industry attributes.
        """
        # create Sub-Sector and Industry objects
        for index, row in self.df.iterrows():
            # create Sub Sector object
            sub_sector = row["GICS_SUB_IND"]
            self.gics[sub_sector] = sectors.GICS(
                class_name=sub_sector, row_information=row
            )
            ss_object = self.gics[sub_sector]

            # create Industry object if not already available
            industry = row["Industry"]
            self.industries[industry] = self.industries.get(
                industry,
                sectors.Industry(
                    industry,
                    transition_risk=row["Transition Risk Module"],
                    **self.transition_params
                ),
            )

            # assign Sub Sector object to Industry and vice verse
            self.industries[industry].add_sub_sector(ss_object)
            ss_object.add_industry(self.industries[industry])

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
