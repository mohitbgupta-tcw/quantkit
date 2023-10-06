import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import numpy as np
import pandas as pd
from copy import deepcopy


class SDGDataSource(ds.DataSources):
    """
    Provide SDG data (sustainability measures) on company level
    Pull data from ISS

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
        super().__init__(params, **kwargs)
        self.sdg = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading SDG Data")
        query = f"""
        SELECT 
            sdga.issuer_id, 
            sdga.issuer_key, 
            sdga.isin AS ISIN,
            sdga.issuer_name AS "IssuerName",
            sdga.ticker AS "Ticker",
            sdga.country_of_incorporation AS "CountryOfIncorporation",
            sdga.ESG_RATING_INDUSTRY AS "ESGRatingIndustry",
            sdga.SDG_SOL_ENERGY_PERCENT_COMB_CONT AS "SDGSolEnergyPercentCombCont",
            sdga.SDG_SOL_CLIMATE_PERCENT_COMB_CONT AS "SDGSolClimatePercentCombCont",
            sdga.SDG_SOL_MAT_USE_PERCENT_COMB_CONT AS "SDGSolMatUsePercentCombCont",
            sdga.SDG_SOL_TERR_ECO_PERCENT_COMB_CONT AS "SDGSolTerrEcoPercentCombCont",
            sdga.SDG_SOL_WATER_PERCENT_COMB_CONT AS "SDGSolWaterPercentCombCont",
            sdga.SDG_SOL_EDU_PERCENT_COMB_CONT AS "SDGSolEduPercentCombCont",
            sdga.SDG_SOL_POVERTY_PERCENT_COMB_CONT AS "SDGSolPovertyPercentCombCont",
            sdga.SDG_SOL_HUNGER_PERCENT_COMB_CONT AS "SDGSolHungerPercentCombCont",
            sdga.SDG_SOL_MARINE_PERCENT_COMB_CONT AS "SDGSolMarinePercentCombCont",
            sdga.SDG_SOL_AGRI_PERCENT_COMB_CONT AS "SDGSolAgriPercentCombCont",
            sdga.SDG_SOL_ENERGY_PROD_COMB_CONT AS "SDGSolEnergyProdCombCont",
            sdga.SDG_SOL_CLIMATE_PROD_COMB_CONT AS "SDGSolClimateProdCombCont",
            sdga.SDG_SOL_MAT_USE_PROD_COMB_CONT AS "SDGSolMatUseProdCombCont",
            sdga.SDG_SOL_TERR_ECO_PROD_COMB_CONT AS "SDGSolTerrEcoProdCombCont",
            sdga.SDG_SOL_WATER_PROD_COMB_CONT AS "SDGSolWaterProdCombCont",
            sdga.SDG_SOL_EDU_PROD_COMB_CONT AS "SDGSolEduProdCombCont",
            sdga.SDG_SOL_POVERTY_PROD_COMB_CONT AS "SDGSolPovertyProdCombCont",
            sdga.SDG_SOL_HUNGER_PROD_COMB_CONT AS "SDGSolHungerProdCombCont",
            sdga.SDG_SOL_MARINE_PROD_COMB_CONT AS "SDGSolMarineProdCombCont",
            sdga.SDG_SOL_AGRI_PROD_COMB_CONT AS "SDGSolAgriProdCombCont",
            sdgaimpact.CLIMATE_GHG_REDUCTION_TARGETS AS "ClimateGHGReductionTargets",
            sdgaimpact.BROWN_EXP_TOTAL_CAP_EX_SHARE_PERCENT AS "BrownExpTotalCapExSharePercent",
            sdgaimpact.GREEN_EXP_TOTAL_CAP_EX_SHARE_PERCENT AS "GreenExpTotalCapExSharePercent"
        FROM tcw_core_qa.esg_iss.fact_esg_issuer_sdga sdga
        LEFT JOIN tcw_core_qa.esg_iss.fact_esg_issuer_sdg_impact_rating sdgaimpact
            ON sdga.ISSUER_KEY = sdgaimpact.ISSUER_KEY
            AND sdga.IS_CURRENT = sdgaimpact.IS_CURRENT
        --AND sdga.LAST_AS_OF_DATE = sdgaimpact.LAST_AS_OF_DATE
        --   WHERE sdga.last_as_of_date = '10/04/2023'
        WHERE sdga.is_current = 1
        ORDER BY sdga.issuer_name
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - replace Not Collected with nan
        - change datatype to float for specified columns
        """
        self.datasource.df = self.datasource.df.replace("Not Collected", np.nan)

        self.datasource.df["GreenExpTotalCapExSharePercent"] = self.datasource.df[
            "GreenExpTotalCapExSharePercent"
        ].astype(float)
        self.datasource.df["SDGSolClimatePercentCombCont"] = self.datasource.df[
            "SDGSolClimatePercentCombCont"
        ].astype(float)

        raise NotImplementedError()

    def iter(self) -> None:
        """
        Attach SDG information to dict
        """
        # only iterate over companies we hold in the portfolios
        for index, row in self.df.iterrows():
            iss_id = str(int(row["issuerID"]))

            sdg_information = row.to_dict()
            self.sdg[iss_id] = sdg_information

        # --> not every company has these information, so create empty df with NA's for those
        empty_sdg = pd.Series(np.nan, index=self.df.columns).to_dict()
        self.sdg[np.nan] = deepcopy(empty_sdg)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
