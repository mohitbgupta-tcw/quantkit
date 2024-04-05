import quantkit.core.data_sources.data_sources as ds
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

    def load(
        self,
        as_of_date: str,
    ) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        as_of_date: str,
            date to pull from API
        """
        logging.log("Loading SDG Data")
        query = f"""
        WITH date_table_sdga AS (
            SELECT issuer_key, MAX(last_as_of_date) AS max_load_date
            FROM tcw_core.esg_iss.fact_esg_issuer_sdga
            WHERE last_as_of_date <= CAST('{as_of_date}' AS DATE)
            GROUP BY issuer_key
        ),
        date_table_sdgaimpact AS (
            SELECT issuer_key, MAX(last_as_of_date) AS max_load_date
            FROM tcw_core.esg_iss.fact_esg_issuer_sdg_impact_rating
            WHERE last_as_of_date <= CAST('{as_of_date}' AS DATE)
            GROUP BY issuer_key
        ), 
        full_query AS (
            SELECT 
                sdga.issuer_id, 
                sdga.issuer_key, 
                sdga.isin AS ISIN,
                sdga.issuer_name AS "IssuerName",
                sdga.ticker AS "Ticker",
                sdga.country_of_incorporation AS "CountryOfIncorporation",
                sdga.esg_rating_industry AS "ESGRatingIndustry",
                sdga.sdg_sol_energy_percent_comb_cont AS "SDGSolEnergyPercentCombCont",
                sdga.sdg_sol_climate_percent_comb_cont AS "SDGSolClimatePercentCombCont",
                sdga.sdg_sol_mat_use_percent_comb_cont AS "SDGSolMatUsePercentCombCont",
                sdga.sdg_sol_terr_eco_percent_comb_cont AS "SDGSolTerrEcoPercentCombCont",
                sdga.sdg_sol_water_percent_comb_cont AS "SDGSolWaterPercentCombCont",
                sdga.sdg_sol_edu_percent_comb_cont AS "SDGSolEduPercentCombCont",
                sdga.sdg_sol_poverty_percent_comb_cont AS "SDGSolPovertyPercentCombCont",
                sdga.sdg_sol_hunger_percent_comb_cont AS "SDGSolHungerPercentCombCont",
                sdga.sdg_sol_marine_percent_comb_cont AS "SDGSolMarinePercentCombCont",
                sdga.sdg_sol_agri_percent_comb_cont AS "SDGSolAgriPercentCombCont",
                sdga.sdg_sol_sus_build_percent_comb_cont "SDGSolSusBuildPercentCombCont",
                sdga.sdg_sol_energy_prod_comb_cont AS "SDGSolEnergyProdCombCont",
                sdga.sdg_sol_climate_prod_comb_cont AS "SDGSolClimateProdCombCont",
                sdga.sdg_sol_mat_use_prod_comb_cont AS "SDGSolMatUseProdCombCont",
                sdga.sdg_sol_terr_eco_prod_comb_cont AS "SDGSolTerrEcoProdCombCont",
                sdga.sdg_sol_water_prod_comb_cont AS "SDGSolWaterProdCombCont",
                sdga.sdg_sol_edu_prod_comb_cont AS "SDGSolEduProdCombCont",
                sdga.sdg_sol_poverty_prod_comb_cont AS "SDGSolPovertyProdCombCont",
                sdga.sdg_sol_hunger_prod_comb_cont AS "SDGSolHungerProdCombCont",
                sdga.sdg_sol_marine_prod_comb_cont AS "SDGSolMarineProdCombCont",
                sdga.sdg_sol_agri_prod_comb_cont AS "SDGSolAgriProdCombCont", 
                sdga.sdg_sol_sus_build_prod_comb_cont "SDGSolSusBuildProdCombCont",
                sdgaimpact.climate_ghg_reduction_targets AS "ClimateGHGReductionTargets",
                sdgaimpact.brown_exp_total_cap_ex_share_percent AS "BrownExpTotalCapExSharePercent",
                sdgaimpact.green_exp_total_cap_ex_share_percent AS "GreenExpTotalCapExSharePercent"
            FROM tcw_core.esg_iss.fact_esg_issuer_sdga sdga
            LEFT JOIN tcw_core.esg_iss.fact_esg_issuer_sdg_impact_rating sdgaimpact
                ON sdga.issuer_key = sdgaimpact.issuer_key
            JOIN date_table_sdga
                ON date_table_sdga.issuer_key = sdga.issuer_key
                AND date_table_sdga.max_load_date = sdga.last_as_of_date
            JOIN date_table_sdgaimpact
                ON date_table_sdgaimpact.issuer_key = sdgaimpact.issuer_key
                AND date_table_sdgaimpact.max_load_date = sdgaimpact.last_as_of_date
        )

        SELECT DISTINCT(issuer_key) AS iss_key, *
        FROM full_query
        ORDER BY issuer_id
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

    def iter(self) -> None:
        """
        Attach SDG information to dict
        """
        # only iterate over companies we hold in the portfolios
        for index, row in self.df.iterrows():
            iss_id = str(int(row["ISSUER_ID"]))

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
