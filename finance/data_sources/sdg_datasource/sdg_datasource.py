import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import numpy as np
import pandas as pd
from copy import deepcopy


class SDGDataSource(object):
    """
    Provide SDG data (sustainability measures) on company level
    Pull data from SDG and SDGA data source

    Parameters
    ----------
    params: dict
        datasource specific parameters including datasources for SDG and SDGA dataset

    Returns
    -------
    DataFrame
        ISIN: str
            company isin
        ESG measures: str | float
            several ESG measures
    """

    def __init__(self, params: dict):
        self.sdg = SDGData(params["sdg"])
        self.sdga = SDGData(params["sdga"])

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading SDG Data")
        self.sdg.datasource.load()
        self.sdg.transform_df()
        self.sdga.datasource.load()
        self.sdga.transform_df()
        self.transform_df()
        return

    def transform_df(self):
        """
        merge sdg and sdga data into one DataFrame
        """
        df_ = self.sdg.datasource.df.merge(
            self.sdga.datasource.df, left_on="ISIN", right_on="ISIN", how="outer"
        )
        df_["GreenExpTotalCapExSharePercent"] = df_[
            "GreenExpTotalCapExSharePercent"
        ].astype(float)
        df_["SDGSolClimatePercentCombCont"] = df_[
            "SDGSolClimatePercentCombCont"
        ].astype(float)
        self.df_ = df_
        return

    def iter(self, companies: dict):
        """
        Attach SDG information to company objects

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        """
        # only iterate over companies we hold in the portfolios
        for index, row in self.df[self.df["ISIN"].isin(companies.keys())].iterrows():
            isin = row["ISIN"]

            sdg_information = row.to_dict()
            companies[isin].sdg_information = sdg_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_sdg = pd.Series(np.nan, index=self.df.columns).to_dict()

        for c in companies:
            # assign empty sdg information to companies that dont have these information
            if not hasattr(companies[c], "sdg_information"):
                companies[c].sdg_information = deepcopy(empty_sdg)
        return

    @property
    def df(self):
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.df_


class SDGData(ds.DataSources):
    """
    Wrapper to load SDG and SDGA data

    Parameters
    ----------
    params: dict
        datasource specific parameters including datasource
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def transform_df(self):
        """
        - drop not needed columns
        - replace 'Not Collected' by nan for every column
        """
        self.datasource.df = self.datasource.df.drop(
            [
                "IssuerName",
                "Ticker",
                "CountryOfIncorporation",
                "issuerID",
                "IssuerLEI",
                "ESGRatingParentEntityName",
                "ESGRatingParentEntityID",
            ],
            axis=1,
        )
        self.datasource.df = self.datasource.df.replace("Not Collected", np.nan)
        return
