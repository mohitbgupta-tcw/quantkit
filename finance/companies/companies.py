import pandas as pd
import numpy as np
import quantkit.finance.securities.securities as securities
import quantkit.finance.sectors.sectors as sectors
import quantkit.finance.adjustment.adjustment as adjustment
import quantkit.finance.transition.transition as transition
import quantkit.finance.data_sources.adjustment_datasource.adjustment_database as adb
import quantkit.finance.data_sources.regions_datasource.regions_datasource as rdb
import quantkit.finance.data_sources.exclusions_datasource.exclusions_database as edb
from typing import Union


class HeadStore(object):
    """
    HeadStore object. Basket for Securities
    Stores information such as:
        - isin
        - attached securities (Equity and Bonds)
        - industry (including GICS and BCLASS objects)
        - scores

    Parameters
    ----------
    isin: str
        company's isin. NoISIN if no isin is available
    regions_datasource: regions_datasource.RegionsDataSource, optional
        regions datasource
    adjustment_datasource: adjustment_database.AdjustmentDataSource, optional
        adjustment datasource
    exclusion_datasource: exclusion_database.ExclusionDataBase, optional
        exclusion datasource
    """

    def __init__(
        self,
        isin: str,
        regions_datasource: rdb.RegionsDataSource = None,
        adjustment_datasource: adb.AdjustmentDataSource = None,
        exclusion_datasource: edb.ExclusionsDataSource = None,
        **kwargs
    ):
        self.isin = isin
        self.securities = dict()
        self.scores = dict()
        self.information = dict()
        self.regions_datasource = regions_datasource
        self.adjustment_datasource = adjustment_datasource
        self.exclusion_datasource = exclusion_datasource

        # assign some default values for measures
        self.scores["Themes"] = dict()
        self.scores["Transition_Category"] = list()
        self.scores["Sustainability_Tag"] = "N"
        self.scores["Transition_Tag"] = "N"
        # self.scores["Sector_Level_2"] = "No Sector"
        self.scores["Muni_Score"] = 0
        self.scores["Sovereign_Score"] = 0
        self.scores["ESRM_Score"] = 0
        self.scores["Governance_Score"] = 0
        self.scores["Target_Score"] = 0
        self.scores["Transition_Score"] = 0
        self.scores["Corporate_Score"] = 0
        self.scores["Review_Flag"] = ""
        self.scores["Review_Comments"] = ""
        self.Adjustment = pd.DataFrame(
            columns=["Thematic Type", "Category", "Adjustment"]
        )
        self.Exclusion = pd.DataFrame()
        self.information["Sector_Level_2"] = np.nan
        self.information["IVA_COMPANY_RATING"] = np.nan
        self.information["Exclusion"] = []

    def add_security(
        self,
        isin: str,
        store: Union[
            securities.EquityStore,
            securities.FixedIncomeStore,
            securities.SecurityStore,
        ],
    ):
        """
        Add security object to parent.
        Security could be stock or issued Fixed Income of company.

        Parameters
        ----------
        isin: str
            security's isin
        store: securities.EquityStore | securities.FixedIncomeStore | securities.SecurityStore:
            security store of new security
        """
        self.securities[isin] = store
        return

    def remove_security(self, isin: str):
        """
        Remove security object from company.

        Parameters
        ----------
        isin: str
            security's isin
        """
        self.securities.pop(isin, None)
        return

    def attach_region(self, regions: dict):
        """
        Attach region information (including ISO2, name, sovereign score) to parent object
        Save region object in self.information["Issuer_Country"]

        Parameters
        ----------
        regions: dict
            dictionary of regions with ISO2 as key and corresponding region object as value
        """
        # dict to map name to ISO2
        temp_regions = pd.Series(
            self.regions_datasource.df.ISO2.values,
            index=self.regions_datasource.df.Country,
        ).to_dict()

        # if issuer country is country name, map to ISO2
        # attach region object to company
        country = self.msci_information["ISSUER_CNTRY_DOMICILE"]
        country = temp_regions.get(country, country)
        self.information["Issuer_Country"] = regions[country]
        regions[country].companies[self.isin] = self
        return

    def analyst_adjustment(self, themes: dict):
        """
        Do analyst adjustments for each parent.
        Different calculations for each thematic type:
            - Risk
            - Transition
            - People
            - Planet
        See quantkit.finance.adjustments for more information

        Parameters
        ----------
        themes: dict
            dictionary of all themes
        """
        # check for analyst adjustment
        for index, row in self.Adjustment.iterrows():
            thematic_type = row["Thematic Type"]
            cat = row["Category"]
            a = row["Adjustment"]
            comment = row["Comments"]
            func_ = getattr(adjustment, thematic_type)
            func_(
                store=self,
                adjustment=a,
                themes=themes,
                theme=cat,
                comment=comment,
            )
        return

    def exclusion(self):
        """
        Do exclusion for each parent.
        Attach if parent would be A8 or A9 excluded
        """
        # check for exclusions
        for index, row in self.Exclusion.iterrows():
            article = row["Article"]
            self.information["Exclusion"].append(article)
        return

    def attach_gics(self, gics_d: dict):
        """
        Attach GICS object to parent store
        Save GICS object in self.information["GICS_SUB_IND"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        # if we can't find GICS in store, create new one as 'Unassigned GICS'
        gics_sub = "Unassigned GICS"
        gics_d[gics_sub] = gics_d.get(
            gics_sub,
            sectors.GICS(gics_sub, pd.Series(gics_d["Unassigned GICS"].information)),
        )
        self.information["GICS_SUB_IND"] = gics_d[gics_sub]
        return


class CompanyStore(HeadStore):
    """
    Company object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)
        - industry (including GICS and BCLASS objects)
        - company and sdg information derived from data providers

    Parameters
    ----------
    isin: str
        company's isin. NoISIN if no isin is available
    row_data: dict
        company information derived from MSCI
    regions_datasource: regions_datasource.RegionsDataSource, optional
        regions datasource
    adjustment_datasource: adjustment_database.AdjustmentDataSource, optional
        adjustment datasource
    exclusion_datasource: exclusion_database.ExclusionDataBase, optional
        exclusion datasource
    """

    def __init__(
        self,
        isin: str,
        row_data: pd.Series,
        regions_datasource: rdb.RegionsDataSource,
        adjustment_datasource: adb.AdjustmentDataSource,
        exclusion_datasource: edb.ExclusionsDataSource,
        **kwargs
    ):
        super().__init__(
            isin,
            regions_datasource=regions_datasource,
            adjustment_datasource=adjustment_datasource,
            exclusion_datasource=exclusion_datasource,
            **kwargs
        )
        self.msci_information = row_data
        self.type = "company"
        self.information["IVA_COMPANY_RATING"] = self.msci_information[
            "IVA_COMPANY_RATING"
        ]

    def update_sovereign_score(self):
        """
        For Treasuries update the Sovereign Score (in self.scores)
        """
        if self.information["BCLASS_Level4"].class_name == "Treasury":
            self.scores["Sovereign_Score"] = self.information[
                "Issuer_Country"
            ].information["Sovereign_Score"]
        return

    def attach_gics(self, gics_d: dict):
        """
        Attach GICS object to parent store
        Save GICS object in self.information["GICS_SUB_IND"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        # if we can't find GICS in store, create new one as 'Unassigned GICS'
        gics_sub = self.msci_information["GICS_SUB_IND"]
        gics_d[gics_sub] = gics_d.get(
            gics_sub,
            sectors.GICS(gics_sub, pd.Series(gics_d["Unassigned GICS"].information)),
        )
        self.information["GICS_SUB_IND"] = gics_d[gics_sub]
        return

    def attach_industry(self, gics_d: dict, bclass_d: dict):
        """
        Attach industry and sub industry object to parent store
        logic: take GICS information, if GICS is unassigned, take BCLASS
        Save industry object in self.information["Industry"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass_level4 with bclass as key, bclass object as value
        """
        gics_sub = self.information["GICS_SUB_IND"].information["GICS_SUB_IND"]
        bclass4 = self.information["BCLASS_Level4"].information["BCLASS_Level4"]

        if gics_sub != "Unassigned GICS":
            gics_object = gics_d[gics_sub]
            self.information["Industry"] = gics_object.industry
            self.information["Sub-Industry"] = gics_object
            self.information["Transition_Risk_Module"] = gics_object.information[
                "Transition Risk Module"
            ]
            # attach company to industry
            gics_object.industry.companies[self.isin] = self
        else:
            bclass_object = bclass_d[bclass4]
            self.information["Industry"] = bclass_object.industry
            self.information["Sub-Industry"] = bclass_object
            self.information["Transition_Risk_Module"] = bclass_object.information[
                "Transition Risk Module"
            ]
            # attach company to industry
            bclass_object.industry.companies[self.isin] = self
        return

    def attach_analyst_adjustment(self):
        """
        Attach analyst adjustment to company object
        Link to adjustment datasource by MSCI issuer id
        """
        # attach analyst adjustment
        msci_issuerid = self.msci_information["ISSUERID"]
        adj_df = self.adjustment_datasource.df[
            self.adjustment_datasource.df["ISIN"] == msci_issuerid
        ]
        if not adj_df.empty:
            self.Adjustment = pd.concat(
                [self.Adjustment, adj_df],
                ignore_index=True,
                sort=False,
            )
        return

    def attach_exclusion(self):
        """
        Attach exclusions from MSCI to company object
        Link to exclusion datasource by MSCI issuer id
        """
        # map exclusion based on Article 8 and 9
        msci_issuerid = self.msci_information["ISSUERID"]
        excl_df = self.exclusion_datasource.df[
            self.exclusion_datasource.df["MSCI Issuer ID"] == msci_issuerid
        ]
        if not excl_df.empty:
            self.Exclusion = pd.concat(
                [self.Exclusion, excl_df],
                ignore_index=True,
                sort=False,
            )
        return

    def calculate_capex(self):
        """
        Calculate the green CapEx of a company
        save capex in information[CapEx]

        Calculation:
            max(
                GreenExpTotalCapExSharePercent,
                RENEW_ENERGY_CAPEX_VS_TOTAL_CAPEX_PCT
            )
        """
        self.information["CapEx"] = np.nanmax(
            [
                self.sdg_information["GreenExpTotalCapExSharePercent"] * 100,
                self.msci_information["RENEW_ENERGY_CAPEX_VS_TOTAL_CAPEX_PCT"],
                0,
            ]
        )
        return

    def calculate_climate_revenue(self):
        """
        Calculate the green climate revenue of a company
        save climate revenue in information[Climate_Revenue]

        Calculation:
            max(
                CT_CC_TOTAL_MAX_REV,
                SDGSolClimatePercentCombCont
            )
        """
        self.information["Climate_Revenue"] = np.nanmax(
            [
                self.msci_information["CT_CC_TOTAL_MAX_REV"],
                self.sdg_information["SDGSolClimatePercentCombCont"] * 100,
                0,
            ]
        )
        return

    def calculate_carbon_intensity(self):
        """
        Calculate the carbon intensity of a company
        save carbon intensity in information[Carbon Intensity (Scope 123)]

        Calculation:
            CARBON_EMISSIONS_SCOPE123 / SALES_USD_RECENT

        Returns
        -------
        bool
            True: carbon intensity couldn't be calculated and company has to be reitered

        """
        reiter = False
        if (
            self.msci_information["SALES_USD_RECENT"] > 0
            and self.msci_information["CARBON_EMISSIONS_SCOPE123"] > 0
        ):
            carbon_intensity = (
                self.msci_information["CARBON_EMISSIONS_SCOPE123"]
                / self.msci_information["SALES_USD_RECENT"]
            )
            self.information["Industry"].update(carbon_intensity)
        # numerator or denominator are zero
        # --> set carbon intensity to NA for now
        # --> replace with median later in reiteration (therefore append to reiter)
        else:
            carbon_intensity = np.nan
            reiter = True

        self.information["Carbon Intensity (Scope 123)"] = carbon_intensity
        return reiter

    def check_theme_requirements(self, themes):
        """
        Iterate over themes and
            - assign theme_ISSKeyAdd in sdg_information
                Logic: company description has theme specific keyword
            - calculate MSCI revenue theme_MSCI
                Logic: sum of theme specific columns
            - calculate ISS revenue theme_ISS
                Logic: max of theme specific columns
            - calculate primary sustainable revenue source in Primary_Rev_Sustainable
                Logic: max revenue across msci and iss and all themes
            - assign theme to company
                Logic: check theme specific requirements
            - assign Sustainability Tag
                Logic: minimum one theme requirement fulfilled

        Parameters
        ----------
        themes: dict
            dictionary of themes with theme as key, corresponding theme object as value
        """

        current_max = 0
        current_sec = 0
        for theme in themes:
            msci_sum = 0
            iss_max = 0
            self.information[theme + "_ISSKeyAdd"] = False
            # sum up MSCI theme specific column to calculate msci revenue
            for sub in themes[theme].msci_sub:
                msci_sum = np.nansum([self.msci_information[sub], msci_sum])
            self.information[theme + "_MSCI"] = msci_sum

            # max over ISS theme specific columns to calculate iss revenue
            for iss in themes[theme].iss_cols:
                iss_max = np.nanmax([iss_max, float(self.sdg_information[iss])])
                # go to Prod column and check for theme specific keywords
                iss_prod = iss.replace("Percent", "Prod")
                description = self.sdg_information[iss_prod]
                if not pd.isna(description):
                    for product_key in themes[theme].product_key_add:
                        if product_key in description:
                            self.information[theme + "_ISSKeyAdd"] = True
            # multiply with 100 to make comparable to MSCI
            iss_max *= 100
            self.information[theme + "_ISS"] = iss_max

            # check if company fulfills theme specific requirements
            func_ = getattr(themes[theme], theme)
            theme_score = func_(
                industry=self.information["Sub-Industry"].class_name,
                iss_key=self.information[theme + "_ISSKeyAdd"],
                msci_rev=msci_sum,
                iss_rev=iss_max,
                capex=self.information["CapEx"],
                palm_tie=self.msci_information["PALM_TIE"],
                orphan_drug_rev=self.msci_information["SI_ORPHAN_DRUGS_REV"],
                acc_to_health=self.msci_information["ACCESS_TO_HLTHCRE_SCORE"],
                trailing_rd_sales=self.bloomberg_information[
                    "TRAILING_12M_R&D_%_SALES"
                ],
                social_fin=self.msci_information["SI_SOCIAL_FIN__MAX_REV"],
                social_connect=self.msci_information["SI_CONNECTIVITY_MAX_REV"],
                social_inclusion=self.msci_information["SI_BASIC_N_TOTAL_MAX_REV"],
            )

            # if requirement fulfilled, assign theme to company and vice versa
            if theme_score:
                themes[theme].companies[self.isin] = self
                self.scores["Themes"][theme] = themes[theme]
                self.scores["Sustainability_Tag"] = "Y"

                # calculate primary sustainable revenue source
                if msci_sum > current_max:
                    self.information["Primary_Rev_Sustainable"] = themes[theme]
                    current_max = msci_sum
                    current_sec = iss_max
                elif msci_sum == current_max and iss_max > current_sec:
                    self.information["Primary_Rev_Sustainable"] = themes[theme]
                    current_max = msci_sum
                    current_sec = iss_max
                if iss_max > current_max:
                    self.information["Primary_Rev_Sustainable"] = themes[theme]
                    current_max = iss_max
                    current_sec = msci_sum
                elif iss_max == current_max and msci_sum > current_sec:
                    self.information["Primary_Rev_Sustainable"] = themes[theme]
                    current_max = iss_max
                    current_sec = msci_sum
        return

    def calculate_esrm_score(self, esrm_d, scoring_d, operators):
        """
        Calculuate esrm score for each company:
        1) For each category save indicator fields and EM and DM flag scorings
        2) For each company:
            2.1) Get ESRM Module (category)
            2.2) Iterate over category indicator fields and
                - count number of flags based on operator and flag threshold
                - save flag value in indicator_Flag
                - create ESRM score based on flag scorings and region
            2.3) Create Governance_Score based on Region_Theme
            2.4) Save flags in company_information

        Parameters
        ----------
        esrm_d: dict
            dictionary with esrm Sub-Sector as key, df filter for that subsector as value
        scoring_d: dict
            dictionary with esrm Sub-Sector as key, scoring metrics for EM and DM as value
        operators: dict
            dictionary of operators translating string to operator object
        """

        counter = 0
        counter_gov = 0
        flag_d = dict()
        gov_d = dict()
        na_d = dict()

        if self.non_applicable_securities():
            self.scores["ESRM_Flags"] = flag_d
            self.scores["Governance_Flags"] = gov_d
            self.scores["NA_Flags"] = na_d
            return

        # get ESRM Module
        sub_industry = self.information["Sub-Industry"]
        esrm = sub_industry.information["ESRM Module"]
        df_ = esrm_d[esrm]
        # count flags
        for index, row in df_.iterrows():
            i = row["Indicator Field Name"]
            v = self.msci_information[i]
            o = row["Operator"]
            ft = row["Flag_Threshold"]
            if pd.isna(v):
                flag_d[i + "_Flag"] = 1
                na_d[i] = 1
                counter += 1
            elif operators[o](v, ft):
                flag_d[i + "_Flag"] = 1
                counter += 1
            else:
                flag_d[i + "_Flag"] = 0

        # specific region for company (DM or EM)
        region = self.information["Issuer_Country"].information["Region_EM"]
        # --> regions have different scoring thresholds
        if region == "DM":
            l = scoring_d[esrm][0]
        else:
            l = scoring_d[esrm][1]

        # create esrm score based on flag scoring
        for i in range(len(l) - 1, -1, -1):
            if counter >= (l[i]):
                self.scores["ESRM_Score"] = i + 1

                if i + 1 == 5:
                    self.scores["Review_Flag"] == "Needs Review"
                break

        # get region theme (DM, JP, EM, EUCohort)
        region_theme = self.information["Issuer_Country"].information["Region"]
        # calculate governance score
        df_ = esrm_d[region_theme]
        for index, row in df_.iterrows():
            i = row["Indicator Field Name"]
            v = self.msci_information[i]
            o = row["Operator"]
            ft = row["Flag_Threshold"]
            if pd.isna(v):
                gov_d[i + "_Flag"] = 1
                na_d[i] = 1
                counter_gov += 1
            elif operators[o](v, ft):
                gov_d[i + "_Flag"] = 1
                counter_gov += 1
            else:
                gov_d[i + "_Flag"] = 0

        for i in range(len(scoring_d[region_theme][0]) - 1, -1, -1):
            if counter_gov >= (scoring_d[region_theme][0][i]):
                self.scores["Governance_Score"] = i + 1

                if i + 1 == 5:
                    self.scores["Review_Flag"] == "Needs Review"
                break

        self.scores["ESRM_Flags"] = flag_d
        self.scores["Governance_Flags"] = gov_d
        self.scores["NA_Flags"] = na_d
        return

    def non_applicable_securities(self):
        """
        For non-applicable securities, apply a score of 0 accross all risk scores

        Rules:
            - Sector Level 2 in excemption list
            - BCLASS is Treasury
            - ISIN is NoISIN
        """
        sector_level2 = ["Cash and Other"]

        if self.information["Sector_Level_2"] in sector_level2:
            return True
        elif self.information["BCLASS_Level4"].class_name == "Treasury":
            return True
        elif self.msci_information["ISSUER_ISIN"] == "NoISIN":
            return True
        elif "TCW" in self.msci_information["ISSUER_NAME"]:
            return True
        elif " ETF " in self.msci_information["ISSUER_NAME"]:
            return True
        return False

    def calculate_transition_score(self):
        """
        Calculate transition score (Transition_Score) for each company:
        0) Check if company is excempted --> set score to 0
        1) Create transition tags
        2) Calculate target score
        3) Calculate transition score
            3.1) Start with industry initial score (3 for low, 5 for high risk)
            3.2) Subtract target score
            3.3) Subtract carbon intensity credit:
                - if in lowest quantile of industry: -2
                - if in medium quantile of industry: -1
        """
        # check for excemption
        if self.non_applicable_securities():
            return

        self.create_transition_tag()
        self.calculate_target_score()

        # subtract target score
        industry = self.information["Industry"]
        transition_score = industry.initial_score + self.scores["Target_Score"]

        # carbon intensity quantile credit
        ci = self.information["Carbon Intensity (Scope 123)"]
        if ci < industry.Q_Low_score:
            transition_score -= 2
        elif ci >= industry.Q_High_score:
            transition_score -= 0
        else:
            transition_score -= 1

        # assign transition score
        transition_score = np.maximum(transition_score, 1)
        self.scores["Transition_Score"] = transition_score

        if transition_score == 5:
            self.scores["Review_Flag"] == "Needs Review"
        return

    def calculate_target_score(self):
        """
        Calculate the target score of a company
        This target score will be used as reduction in transition score calculation
        if company fulfills one of the rules below, transition score will be reduced by one
        save target score in company_information[Target_Score]

        Rules:
            - ClimateGHGReductionTargets is 'Ambitious Target', 'Approved SBT', or 'Committed SBT'
            - CapEx >= 30
            - HAS_COMMITTED_TO_SBTI_TARGET is True
            - HAS_SBTI_APPROVED_TARGET is True
        """
        if self.sdg_information["ClimateGHGReductionTargets"] in [
            "Ambitious Target",
            "Approved SBT",
            "Committed SBT",
        ]:
            target_score = -1
        elif self.information["CapEx"] >= 30:
            target_score = -1
        elif self.msci_information["HAS_COMMITTED_TO_SBTI_TARGET"] == 1:
            target_score = -1
        elif self.msci_information["HAS_SBTI_APPROVED_TARGET"] == 1:
            target_score = -1
        else:
            target_score = 0
        self.scores["Target_Score"] = target_score
        return

    def create_transition_tag(self):
        """
        create transition tags by:
        1) If company's sub industry is in removal list and already is sustainable, skipp
        2) check if company fulfills sub-sectors transition target
        3) check if company fulfills sub-sectors transition revenue target
        4) if 2) or 3), set Transition_Tag to 'Y' and Transition_Category to transition acronym
        """

        # check removal condition
        Remove_Transition_Set = [
            "Industrial Machinery",
            "Motorcycle Manufacturers",
            "Highways & Railtracks",
            "Industrial Conglomerates",
            "Electrical Components & Equipment",
            "Heavy Electrical Equipment",
        ]
        if (
            self.information["Sub-Industry"].class_name in Remove_Transition_Set
            and self.scores["Sustainability_Tag"] == "Y"
        ):
            return

        # check if company's sub industry has transition plan
        if hasattr(self.information["Sub-Industry"], "transition"):
            transition_target = self.information["Sub-Industry"].transition[
                "Transition_Target"
            ]
            transition_revenue = self.information["Sub-Industry"].transition[
                "Transition_Revenue"
            ]

            # check requirements for transition target
            if not pd.isna(transition_target):
                func_ = getattr(transition, transition_target)
                transition_ = func_(
                    reduction_target=self.sdg_information["ClimateGHGReductionTargets"],
                    sbti_commited_target=self.msci_information[
                        "HAS_COMMITTED_TO_SBTI_TARGET"
                    ],
                    sbti_approved_target=self.msci_information[
                        "HAS_SBTI_APPROVED_TARGET"
                    ],
                    capex=self.information["CapEx"],
                    climate_rev=self.information["Climate_Revenue"],
                    biofuel_rev=self.msci_information["CT_ALT_ENERGY_BIOFUEL_MAX_REV"],
                    alt_energy_rev=self.msci_information["CT_ALT_ENERGY_MAX_REV"],
                    thermal_coal_rev=self.msci_information["THERMAL_COAL_MAX_REV_PCT"],
                    company_name=self.msci_information["ISSUER_NAME"],
                )
                # transition requirements fulfilled
                if transition_:
                    self.scores["Transition_Tag"] = "Y"
                    self.scores["Transition_Category"].append(
                        self.information["Sub-Industry"].transition["Acronym"]
                    )
                    return

            # check requirements for transition revenue target
            if not pd.isna(transition_revenue):
                level = int(transition_revenue.split("_")[1])
                transition_ = transition.Revenue(
                    climate_rev=self.information["Climate_Revenue"],
                    capex=self.information["CapEx"],
                    revenue_threshold=level,
                )
                if transition_:
                    self.scores["Transition_Tag"] = "Y"
                    self.scores["Transition_Category"].append(
                        self.information["Sub-Industry"].transition["Acronym"]
                    )
        return

    def calculate_corporate_score(self):
        """
        Calculate corporate score for a company based on other scores.
        Calculation:

            (Governance Score + ESRM Score + Transition Score) / 3
        """
        self.scores["Corporate_Score"] = np.mean(
            [
                self.scores["ESRM_Score"],
                self.scores["Governance_Score"],
                self.scores["Transition_Score"],
            ]
        )
        return

    def calculate_risk_overall_score(self):
        """
        Calculate risk overall score on security level:
            - if corporate score between 1 and 2: Leading
            - if corporate score between 2 and 4: Average
            - if corporate score above 4: Poor
            - if corporate score 0: not scored
        """
        score = self.scores["Corporate_Score"]
        for s in self.securities:
            if score >= 1 and score <= 2:
                self.securities[s].scores["Risk_Score_Overall"] = "Leading ESG Score"
            elif score > 2 and score <= 4:
                self.securities[s].scores["Risk_Score_Overall"] = "Average ESG Score"
            elif score == 0:
                self.securities[s].scores["Risk_Score_Overall"] = "Not Scored"
        return

    def update_sclass(self):
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based
        """
        transition_score = self.scores["Transition_Score"]
        governance_score = self.scores["Governance_Score"]
        esrm_score = self.scores["ESRM_Score"]
        score_sum = governance_score + transition_score + esrm_score
        transition_category = self.scores["Transition_Category"]
        transition_tag = self.scores["Transition_Tag"]
        sustainability_category = self.scores["Themes"]
        themes = list(sustainability_category.keys())
        sustainability_tag = self.scores["Sustainability_Tag"]
        for s in self.securities:
            self.securities[s].information["SClass_Level5"] = self.securities[
                s
            ].information["ESG_Collateral_Type"]["ESG Collat Type"]

            if self.securities[s].information["Labeled_ESG_Type"] == "Labeled Green":
                self.securities[s].information["SClass_Level4-P"] = "Green"
                self.securities[s].information["SClass_Level4"] = "Green"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif self.securities[s].information["Labeled_ESG_Type"] == "Labeled Social":
                self.securities[s].information["SClass_Level4-P"] = "Social"
                self.securities[s].information["SClass_Level4"] = "Social"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable"
            ):
                self.securities[s].information["SClass_Level4-P"] = "Sustainable"
                self.securities[s].information["SClass_Level4"] = "Sustainable"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable Linked"
            ):
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information[
                    "SClass_Level4"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"

            elif sustainability_tag == "Y*":
                self.securities[s].information["SClass_Level2"] = "Sustainable Theme"
                self.securities[s].information["SClass_Level1"] = "Preferred"
                if len(sustainability_category) == 1:
                    theme = themes[0]
                    self.securities[s].information["SClass_Level4"] = theme
                    self.securities[s].information["SClass_Level4-P"] = theme
                    self.securities[s].information[
                        "SClass_Level3"
                    ] = sustainability_category[theme].pillar
                elif len(sustainability_category) >= 2:
                    self.securities[s].information["SClass_Level3"] = "Multi-Thematic"
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = self.information["Primary_Rev_Sustainable"].acronym
                    people_count = 0
                    planet_count = 0
                    for t in sustainability_category:
                        if sustainability_category[t].pillar == "People":
                            people_count += 1
                        elif sustainability_category[t].pillar == "Planet":
                            planet_count += 1

                        if planet_count >= 1 and people_count >= 1:
                            self.securities[s].information[
                                "SClass_Level4"
                            ] = "Planet & People"
                        elif planet_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "Planet"
                        elif people_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "People"
                else:
                    raise ValueError(
                        "If sustainability tag is 'Y*', theme should be assigned."
                    )

            elif transition_tag == "Y*":
                self.securities[s].information["SClass_Level3"] = "Transition"
                self.securities[s].information["SClass_Level2"] = "Transition"
                self.securities[s].information["SClass_Level1"] = "Eligible"
                if len(transition_category) > 1:
                    self.securities[s].information["SClass_Level4"] = "Multi-Thematic"
                    self.securities[s].information["SClass_Level4-P"] = "Multi-Thematic"
                elif len(transition_category) == 1:
                    self.securities[s].information[
                        "SClass_Level4"
                    ] = transition_category[0]
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = transition_category[0]
                else:
                    raise ValueError(
                        "If transition tag is 'Y*', category should be assigned."
                    )

            elif governance_score == 5:
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Poor Governance Score"
                self.securities[s].information[
                    "SClass_Level4"
                ] = "Poor Governance Score"
                self.securities[s].information["SClass_Level3"] = "Exclusion"
                self.securities[s].information["SClass_Level2"] = "Exclusion"
                self.securities[s].information["SClass_Level1"] = "Excluded"

            elif (
                esrm_score == 5
                or self.msci_information["UNGC_COMPLIANCE"] == "Fail"
                or self.msci_information["OVERALL_FLAG"] == "Red"
                or self.msci_information["IVA_COMPANY_RATING"] == "CCC"
            ):
                self.securities[s].information["SClass_Level4-P"] = "Poor ESRM Score"
                self.securities[s].information["SClass_Level4"] = "Poor ESRM Score"
                self.securities[s].information["SClass_Level3"] = "Exclusion"
                self.securities[s].information["SClass_Level2"] = "Exclusion"
                self.securities[s].information["SClass_Level1"] = "Excluded"
                self.scores["ESRM_Score"] = 5

            elif transition_score == 5:
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Poor Transition Score"
                self.securities[s].information[
                    "SClass_Level4"
                ] = "Poor Transition Score"
                self.securities[s].information["SClass_Level3"] = "Exclusion"
                self.securities[s].information["SClass_Level2"] = "Exclusion"
                self.securities[s].information["SClass_Level1"] = "Excluded"

            elif sustainability_tag == "Y":
                self.securities[s].information["SClass_Level2"] = "Sustainable Theme"
                self.securities[s].information["SClass_Level1"] = "Preferred"
                if len(sustainability_category) == 1:
                    theme = themes[0]
                    self.securities[s].information["SClass_Level4"] = theme
                    self.securities[s].information["SClass_Level4-P"] = theme
                    self.securities[s].information[
                        "SClass_Level3"
                    ] = sustainability_category[theme].pillar
                elif len(sustainability_category) >= 2:
                    self.securities[s].information["SClass_Level3"] = "Multi-Thematic"
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = self.information["Primary_Rev_Sustainable"].acronym
                    people_count = 0
                    planet_count = 0
                    for t in sustainability_category:
                        if sustainability_category[t].pillar == "People":
                            people_count += 1
                        elif sustainability_category[t].pillar == "Planet":
                            planet_count += 1

                        if planet_count >= 1 and people_count >= 1:
                            self.securities[s].information[
                                "SClass_Level4"
                            ] = "Planet & People"
                        elif planet_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "Planet"
                        elif people_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "People"
                else:
                    raise ValueError(
                        "If sustainability tag is 'Y', theme should be assigned."
                    )

            elif transition_tag == "Y":
                self.securities[s].information["SClass_Level3"] = "Transition"
                self.securities[s].information["SClass_Level2"] = "Transition"
                self.securities[s].information["SClass_Level1"] = "Eligible"
                if len(transition_category) > 1:
                    self.securities[s].information["SClass_Level4"] = "Multi-Thematic"
                    self.securities[s].information["SClass_Level4-P"] = "Multi-Thematic"
                elif len(transition_category) == 1:
                    self.securities[s].information[
                        "SClass_Level4"
                    ] = transition_category[0]
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = transition_category[0]
                else:
                    raise ValueError(
                        "If transition tag is 'Y', category should be assigned."
                    )

            elif score_sum <= 6 and score_sum > 0:
                self.securities[s].information["SClass_Level4"] = "Leading ESG Score"
                self.securities[s].information["SClass_Level4-P"] = "Leading ESG Score"
            elif score_sum == 0:
                self.securities[s].information["SClass_Level4"] = "Not Scored"
                self.securities[s].information["SClass_Level4-P"] = "Not Scored"

        return


class MuniStore(HeadStore):
    """
    Muni object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        muni's isin. NoISIN if no isin is available
    """

    def __init__(self, isin: str, **kwargs):
        super().__init__(isin, **kwargs)
        self.type = "muni"

    def calculate_risk_overall_score(self):
        """
        Calculate risk overall score on security level:
            - if muni score between 1 and 2: Leading
            - if muni score between 2 and 4: Average
            - if muni score above 4: Poor
            - if muni score 0: not scored
        """
        score = self.scores["Muni_Score"]
        for s in self.securities:
            if score in [1, 2]:
                self.securities[s].scores["Risk_Score_Overall"] = "Leading ESG Score"
            elif score in [3, 4]:
                self.securities[s].scores["Risk_Score_Overall"] = "Average ESG Score"
            elif score == 0:
                self.securities[s].scores["Risk_Score_Overall"] = "Not Scored"
        return

    def update_sclass(self):
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based
        """
        score = self.scores["Muni_Score"]
        transition_category = self.scores["Transition_Category"]
        transition_tag = self.scores["Transition_Tag"]
        sustainability_category = self.scores["Themes"]
        themes = list(sustainability_category.keys())
        sustainability_tag = self.scores["Sustainability_Tag"]
        for s in self.securities:
            self.securities[s].information["SClass_Level5"] = self.securities[
                s
            ].information["ESG_Collateral_Type"]["ESG Collat Type"]

            if self.securities[s].information["Labeled_ESG_Type"] == "Labeled Green":
                self.securities[s].information["SClass_Level4-P"] = "Green"
                self.securities[s].information["SClass_Level4"] = "Green"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif self.securities[s].information["Labeled_ESG_Type"] == "Labeled Social":
                self.securities[s].information["SClass_Level4-P"] = "Social"
                self.securities[s].information["SClass_Level4"] = "Social"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable"
            ):
                self.securities[s].information["SClass_Level4-P"] = "Sustainable"
                self.securities[s].information["SClass_Level4"] = "Sustainable"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable Linked"
            ):
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information[
                    "SClass_Level4"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"

            elif sustainability_tag == "Y*":
                self.securities[s].information["SClass_Level2"] = "Sustainable Theme"
                self.securities[s].information["SClass_Level1"] = "Preferred"
                if len(sustainability_category) == 1:
                    theme = themes[0]
                    self.securities[s].information["SClass_Level4"] = theme
                    self.securities[s].information["SClass_Level4-P"] = theme
                    self.securities[s].information[
                        "SClass_Level3"
                    ] = sustainability_category[theme].pillar
                elif len(sustainability_category) >= 2:
                    self.securities[s].information["SClass_Level3"] = "Multi-Thematic"
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = self.information["Primary_Rev_Sustainable"].acronym
                    people_count = 0
                    planet_count = 0
                    for t in sustainability_category:
                        if sustainability_category[t].pillar == "People":
                            people_count += 1
                        elif sustainability_category[t].pillar == "Planet":
                            planet_count += 1

                        if planet_count >= 1 and people_count >= 1:
                            self.securities[s].information[
                                "SClass_Level4"
                            ] = "Planet & People"
                        elif planet_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "Planet"
                        elif people_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "People"
                else:
                    raise ValueError(
                        "If sustainability tag is 'Y', theme should be assigned."
                    )

            elif transition_tag == "Y*":
                self.securities[s].information["SClass_Level3"] = "Transition"
                self.securities[s].information["SClass_Level2"] = "Transition"
                self.securities[s].information["SClass_Level1"] = "Eligible"
                if len(transition_category) > 1:
                    self.securities[s].information["SClass_Level4"] = "Multi-Thematic"
                    self.securities[s].information["SClass_Level4-P"] = "Multi-Thematic"
                elif len(transition_category) == 1:
                    self.securities[s].information[
                        "SClass_Level4"
                    ] = transition_category[0]
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = transition_category[0]
                else:
                    raise ValueError(
                        "If transition tag is 'Y', category should be assigned."
                    )

            elif score == 5:
                self.securities[s].information["SClass_Level4-P"] = "Poor Muni Score"
                self.securities[s].information["SClass_Level4"] = "Poor Muni Score"
                self.securities[s].information["SClass_Level3"] = "Exclusion"
                self.securities[s].information["SClass_Level2"] = "Exclusion"
                self.securities[s].information["SClass_Level1"] = "Excluded"

            elif sustainability_tag == "Y":
                self.securities[s].information["SClass_Level2"] = "Sustainable Theme"
                self.securities[s].information["SClass_Level1"] = "Preferred"
                if len(sustainability_category) == 1:
                    theme = themes[0]
                    self.securities[s].information["SClass_Level4"] = theme
                    self.securities[s].information["SClass_Level4-P"] = theme
                    self.securities[s].information[
                        "SClass_Level3"
                    ] = sustainability_category[theme].pillar
                elif len(sustainability_category) >= 2:
                    self.securities[s].information["SClass_Level3"] = "Multi-Thematic"
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = self.information["Primary_Rev_Sustainable"].acronym
                    people_count = 0
                    planet_count = 0
                    for t in sustainability_category:
                        if sustainability_category[t].pillar == "People":
                            people_count += 1
                        elif sustainability_category[t].pillar == "Planet":
                            planet_count += 1

                        if planet_count >= 1 and people_count >= 1:
                            self.securities[s].information[
                                "SClass_Level4"
                            ] = "Planet & People"
                        elif planet_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "Planet"
                        elif people_count >= 2:
                            self.securities[s].information["SClass_Level4"] = "People"
                else:
                    raise ValueError(
                        "If sustainability tag is 'Y', theme should be assigned."
                    )

            elif transition_tag == "Y":
                self.securities[s].information["SClass_Level3"] = "Transition"
                self.securities[s].information["SClass_Level2"] = "Transition"
                self.securities[s].information["SClass_Level1"] = "Eligible"
                if len(transition_category) > 1:
                    self.securities[s].information["SClass_Level4"] = "Multi-Thematic"
                    self.securities[s].information["SClass_Level4-P"] = "Multi-Thematic"
                elif len(transition_category) == 1:
                    self.securities[s].information[
                        "SClass_Level4"
                    ] = transition_category[0]
                    self.securities[s].information[
                        "SClass_Level4-P"
                    ] = transition_category[0]
                else:
                    raise ValueError(
                        "If transition tag is 'Y', category should be assigned."
                    )

            elif score <= 2 and score > 0:
                self.securities[s].information["SClass_Level4"] = "Leading ESG Score"
                self.securities[s].information["SClass_Level4-P"] = "Leading ESG Score"
            elif score == 0:
                self.securities[s].information["SClass_Level4"] = "Not Scored"
                self.securities[s].information["SClass_Level4-P"] = "Not Scored"
        return


class SecuritizedStore(HeadStore):
    """
    Securitized object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        securitized's isin. NoISIN if no isin is available
    """

    def __init__(self, isin: str, **kwargs):
        super().__init__(isin, **kwargs)
        self.type = "securitized"

    def calculate_securitized_score(self):
        """
        Calculation of Securitized Score (on security level)
        """
        collat_type_1 = [
            "LEED Platinum",
            "LEED Gold",
            "LEED Silver",
            "LEED Certified",
            "LEED (Multi Property)",
            "BREEAM Very Good",
        ]
        collat_type_2 = ["TCW Criteria", "Small Business Loan", "FFELP Student Loan"]
        for s in self.securities:
            sec_store = self.securities[s]

            if (
                not pd.isna(sec_store.information["Labeled_ESG_Type"])
            ) or sec_store.information["Issuer_ESG"] == "Yes":
                if pd.isna(sec_store.information["TCW_ESG"]):
                    sec_store.scores["Securitized_Score"] = 5
                    continue
                else:
                    sec_store.scores["Securitized_Score"] = 1
                    continue
            elif (
                sec_store.information["ESG_Collateral_Type"]["ESG Collat Type"]
                in collat_type_1
                and sec_store.information["Labeled_ESG_Type"] != "Labeled Green"
                and sec_store.information["TCW_ESG"] == "TCW Green"
            ):
                sec_store.scores["Securitized_Score"] = 2
                continue
            elif (
                sec_store.information["ESG_Collateral_Type"]["ESG Collat Type"]
                in collat_type_2
                and sec_store.information["Labeled_ESG_Type"] != "Labeled Social"
                and sec_store.information["TCW_ESG"] == "TCW Social"
                and not "TBA " in sec_store.information["Issuer_Name"]
            ):
                sec_store.scores["Securitized_Score"] = 2
                continue
            elif (
                sec_store.information["ESG_Collateral_Type"]["ESG Collat Type"]
                == "ESG CLO"
            ):
                sec_store.scores["Securitized_Score"] = 2
                continue
            elif "TBA " in sec_store.information["Issuer_Name"]:
                sec_store.scores["Securitized_Score"] = 3
            elif (
                (pd.isna(sec_store.information["Labeled_ESG_Type"]))
                and pd.isna(sec_store.information["TCW_ESG"])
                and not "TBA " in sec_store.information["Issuer_Name"]
            ):
                sec_store.scores["Securitized_Score"] = 4
                continue

        return

    def calculate_risk_overall_score(self):
        """
        Calculate risk overall score on security level:
            - if securitized score between 1 and 2: Leading
            - if securitized score between 2 and 4: Average
            - if securitized score above 4: Poor
            - if securitized score 0: not scored
        """
        for s in self.securities:
            score = self.securities[s].scores["Securitized_Score"]
            if score in [1, 2]:
                self.securities[s].scores["Risk_Score_Overall"] = "Leading ESG Score"
            elif score in [3, 4]:
                self.securities[s].scores["Risk_Score_Overall"] = "Average ESG Score"
            elif score == 0:
                self.securities[s].scores["Risk_Score_Overall"] = "Not Scored"
        return

    def update_sclass(self):
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based
        """
        for s in self.securities:
            score = self.securities[s].scores["Securitized_Score"]
            self.securities[s].information["SClass_Level5"] = self.securities[
                s
            ].information["ESG_Collateral_Type"]["ESG Collat Type"]

            if self.securities[s].information["Labeled_ESG_Type"] == "Labeled Green":
                self.securities[s].information["SClass_Level4-P"] = "Green"
                self.securities[s].information["SClass_Level4"] = "Green"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif self.securities[s].information["Labeled_ESG_Type"] == "Labeled Social":
                self.securities[s].information["SClass_Level4-P"] = "Social"
                self.securities[s].information["SClass_Level4"] = "Social"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable"
            ):
                self.securities[s].information["SClass_Level4-P"] = "Sustainable"
                self.securities[s].information["SClass_Level4"] = "Sustainable"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable Linked"
            ):
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information[
                    "SClass_Level4"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"

            elif score == 5:
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Poor Securitized Score"
                self.securities[s].information[
                    "SClass_Level4"
                ] = "Poor Securitized Score"
                self.securities[s].information["SClass_Level3"] = "Exclusion"
                self.securities[s].information["SClass_Level2"] = "Exclusion"
                self.securities[s].information["SClass_Level1"] = "Excluded"

            elif (
                not self.securities[s].information["ESG_Collateral_Type"][
                    "ESG Collat Type"
                ]
                == "Unknown"
            ):
                self.securities[s].information["SClass_Level4-P"] = self.securities[
                    s
                ].information["ESG_Collateral_Type"]["Primary"]
                self.securities[s].information["SClass_Level4"] = self.securities[
                    s
                ].information["ESG_Collateral_Type"]["Primary"]
                self.securities[s].information["SClass_Level3"] = self.securities[
                    s
                ].information["ESG_Collateral_Type"]["Sclass_Level3"]
                self.securities[s].information["SClass_Level2"] = "Sustainable Theme"
                self.securities[s].information["SClass_Level1"] = "Preferred"

            elif " TBA " in self.securities[s].information["Issuer_Name"]:
                self.securities[s].information["SClass_Level4-P"] = "AFFORDABLE"
                self.securities[s].information["SClass_Level4"] = "AFFORDABLE"
                self.securities[s].information["SClass_Level3"] = "People"
                self.securities[s].information["SClass_Level2"] = "Sustainable Theme"
                self.securities[s].information["SClass_Level1"] = "Preferred"

            elif score <= 2 and score > 0:
                self.securities[s].information["SClass_Level4"] = "Leading ESG Score"
                self.securities[s].information["SClass_Level4-P"] = "Leading ESG Score"
            elif score == 0:
                self.securities[s].information["SClass_Level4"] = "Not Scored"
                self.securities[s].information["SClass_Level4-P"] = "Not Scored"
        return


class SovereignStore(HeadStore):
    """
    Sovereign object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        company's isin. NoISIN if no isin is available
    row_data: pd.Series
        company information derived from SSI and MSCI
    regions_datasource: regions_datasource.RegionsDataSource, optional
        regions datasource
    adjustment_datasource: adjustment_database.AdjustmentDataSource, optional
        adjustment datasource
    """

    def __init__(
        self,
        isin: str,
        regions_datasource: rdb.RegionsDataSource,
        adjustment_datasource: adb.AdjustmentDataSource,
        **kwargs
    ):
        super().__init__(
            isin,
            regions_datasource=regions_datasource,
            adjustment_datasource=adjustment_datasource,
            **kwargs
        )
        self.msci_information = {}
        self.type = "sovereign"

    def update_sovereign_score(self):
        """
        Set Sovereign Score
        """
        self.scores["Sovereign_Score"] = self.information["Issuer_Country"].information[
            "Sovereign_Score"
        ]
        return

    def attach_analyst_adjustment(self):
        """
        Attach analyst adjustment to sovereign object
        Link to adjustment datasource by MSCI issuer id
        """
        # attach analyst adjustment
        msci_issuerid = self.msci_information["ISSUERID"]
        adj_df = self.adjustment_datasource.df[
            self.adjustment_datasource.df["ISIN"] == msci_issuerid
        ]
        if not adj_df.empty:
            self.Adjustment = pd.concat(
                [self.Adjustment, adj_df],
                ignore_index=True,
                sort=False,
            )
        return

    def attach_gics(self, gics_d: dict):
        """
        Attach GICS object to parent store
        Save GICS object in self.information["GICS_SUB_IND"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        # if we can't find GICS in store, create new one as 'Unassigned GICS'
        gics_sub = self.msci_information["GICS_SUB_IND"]
        gics_d[gics_sub] = gics_d.get(
            gics_sub,
            sectors.GICS(gics_sub, pd.Series(gics_d["Unassigned GICS"].information)),
        )
        self.information["GICS_SUB_IND"] = gics_d[gics_sub]
        return

    def calculate_risk_overall_score(self):
        """
        Calculate risk overall score on security level:
            - if sovereign score between 1 and 2: Leading
            - if sovereign score between 2 and 4: Average
            - if sovereign score above 4: Poor
            - if sovereign score 0: not scored
        """
        score = self.scores["Sovereign_Score"]
        for s in self.securities:
            if score in [1, 2]:
                self.securities[s].scores["Risk_Score_Overall"] = "Leading ESG Score"
            elif score in [3, 4]:
                self.securities[s].scores["Risk_Score_Overall"] = "Average ESG Score"
            elif score == 0:
                self.securities[s].scores["Risk_Score_Overall"] = "Not Scored"
        return

    def update_sclass(self):
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based
        """
        score = self.scores["Sovereign_Score"]
        for s in self.securities:
            self.securities[s].information["SClass_Level5"] = self.securities[
                s
            ].information["ESG_Collateral_Type"]["ESG Collat Type"]

            if self.securities[s].information["Labeled_ESG_Type"] == "Labeled Green":
                self.securities[s].information["SClass_Level4-P"] = "Green"
                self.securities[s].information["SClass_Level4"] = "Green"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif self.securities[s].information["Labeled_ESG_Type"] == "Labeled Social":
                self.securities[s].information["SClass_Level4-P"] = "Social"
                self.securities[s].information["SClass_Level4"] = "Social"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable"
            ):
                self.securities[s].information["SClass_Level4-P"] = "Sustainable"
                self.securities[s].information["SClass_Level4"] = "Sustainable"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable Linked"
            ):
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information[
                    "SClass_Level4"
                ] = "Sustainability-Linked Bonds"
                self.securities[s].information["SClass_Level3"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level2"] = "ESG-Labeled Bonds"
                self.securities[s].information["SClass_Level1"] = "Preferred"

            elif score <= 2 and score > 0:
                self.securities[s].information["SClass_Level4"] = "Leading ESG Score"
                self.securities[s].information["SClass_Level4-P"] = "Leading ESG Score"
            elif score == 0:
                self.securities[s].information["SClass_Level4"] = "Not Scored"
                self.securities[s].information["SClass_Level4-P"] = "Not Scored"
            elif score == 5:
                self.securities[s].information[
                    "SClass_Level4-P"
                ] = "Poor Sovereign Score"
                self.securities[s].information["SClass_Level4"] = "Poor Sovereign Score"
                self.securities[s].information["SClass_Level3"] = "Exclusion"
                self.securities[s].information["SClass_Level2"] = "Exclusion"
                self.securities[s].information["SClass_Level1"] = "Excluded"
        return
