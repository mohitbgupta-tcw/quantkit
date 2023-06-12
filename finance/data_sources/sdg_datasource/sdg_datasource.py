import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import numpy as np


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
        logging.log("Loading SDG Data")
        self.sdg = SDGData(params["sdg"])
        self.sdga = SDGData(params["sdga"])

        self.df_ = self.transform_df()

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
        return df_

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
