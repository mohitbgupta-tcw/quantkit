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

    def __init__(self, params: dict, **kwargs) -> None:
        self.sdg_datasource = SDGData(params["sdg"], **kwargs)
        self.sdga_datasource = SDGData(params["sdga"], **kwargs)
        self.sdg = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading SDG Data")
        self.sdg_datasource.load()
        self.sdga_datasource.load()
        self.transform_df()

    def transform_df(self) -> None:
        """
        - merge sdg and sdga data into one DataFrame
        - change datatype to float for specified columns
        """
        df_ = self.sdg_datasource.datasource.df.merge(
            self.sdga_datasource.datasource.df,
            left_on="ISIN",
            right_on="ISIN",
            how="outer",
        )
        df_["GreenExpTotalCapExSharePercent"] = df_[
            "GreenExpTotalCapExSharePercent"
        ].astype(float)
        df_["SDGSolClimatePercentCombCont"] = df_[
            "SDGSolClimatePercentCombCont"
        ].astype(float)

        df_["issuerID"] = df_["issuerID_x"].fillna(df_["issuerID_y"])
        df_ = df_.drop(["issuerID_x", "issuerID_y"], axis=1)

        self.df_ = df_

    def iter(
        self, companies: dict, munis: dict, sovereigns: dict, securitized: dict
    ) -> None:
        """
        Attach SDG information to company objects

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        munis: dict
            dictionary of all muni objects
        sovereigns: dict
            dictionary of all sovereign objects
        securitized: dict
            dictionary of all securitized objects
        """
        # only iterate over companies we hold in the portfolios
        for index, row in self.df[self.df["ISIN"].isin(companies.keys())].iterrows():
            isin = row["ISIN"]
            iss_id = row["issuerID"]

            sdg_information = row.to_dict()
            self.sdg[iss_id] = sdg_information
            companies[isin].sdg_information = sdg_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_sdg = pd.Series(np.nan, index=self.df.columns).to_dict()
        self.sdg[np.nan] = empty_sdg

        parents = [companies, munis, sovereigns, securitized]

        for p in parents:
            for s, sec_store in p.items():
                # assign empty sdg information to companies that dont have these information
                if not hasattr(sec_store, "sdg_information"):
                    sec_store.sdg_information = deepcopy(empty_sdg)

    @property
    def df(self) -> pd.DataFrame:
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

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - drop not needed columns
        - replace 'Not Collected' by nan for every column
        """
        self.datasource.df = self.datasource.df.drop(
            [
                "IssuerName",
                "Ticker",
                "CountryOfIncorporation",
                "IssuerLEI",
                "ESGRatingParentEntityName",
                "ESGRatingParentEntityID",
            ],
            axis=1,
        )
        self.datasource.df = self.datasource.df.replace("Not Collected", np.nan)
