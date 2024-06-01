import numpy as np
import pandas as pd
from pandas import Series
from copy import deepcopy
from quantkit.core.characteristics.sector_level2 import (
    CorporateStore,
    CashStore,
    MuniStore,
    SovereignStore,
    SecuritizedStore,
)
import quantkit.risk_framework.core.transition as transition
import quantkit.utils.util_functions as util_functions


class CorporateStore(CorporateStore):
    """
    Corporate Sector object.

    Parameters
    ----------
    sector_level2: str
        sector level 2 label
    """

    def attach_transition_info(
        self,
        transition_company_mapping: dict,
    ) -> None:
        """
        Overwrite Enabler/Improver and Acronym tag for company specific companies

        Parameters
        ----------
        transition_company_mapping: dict
            companies with company specific improver/enabler tag
        companies: dict
            dictionary of all company objects
        """
        if hasattr(self.information["Sub-Industry"], "information"):
            transition_info = deepcopy(self.information["Sub-Industry"].information)
        else:
            transition_info = dict()
        if self.isin in transition_company_mapping:
            transition_info["ENABLER_IMPROVER"] = transition_company_mapping[self.isin][
                "ENABLER_IMPROVER"
            ]
            transition_info["ACRONYM"] = transition_company_mapping[self.isin][
                "ACRONYM"
            ]
        self.information["transition_info"] = transition_info

    def update_sovereign_score(self) -> None:
        """
        For Treasuries update the Sovereign Score (in self.scores)
        """
        if self.information["BCLASS_Level4"].class_name == "Treasury":
            self.scores["Sovereign_Score_unadjusted"] = deepcopy(
                self.information["Issuer_Country"].information["Sovereign_Score"]
            )
            self.scores["Sovereign_Score"] = self.information[
                "Issuer_Country"
            ].information["Sovereign_Score"]

    def calculate_capex(self) -> None:
        """
        Calculate the green CapEx of a company
        save capex in information[CapEx]

        Calculation
        -----------
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

    def calculate_climate_revenue(self) -> None:
        """
        Calculate the green climate revenue of a company
        save climate revenue in information[Climate_Revenue]

        Calculation
        -----------
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

    def calculate_climate_decarb(self) -> None:
        """
        Calculate the decarbonization improvement of a company
        """
        if self.msci_information["SALES_USD_RECENT"] > 0:
            carbon_rn = (
                self.msci_information["CARBON_EMISSIONS_SCOPE_12_INTEN"]
                + self.msci_information["CARBON_EMISSIONS_SCOPE_3"]
                / self.msci_information["SALES_USD_RECENT"]
            )
        else:
            carbon_rn = 0

        if self.msci_information["SALES_USD_FY19"] > 0:
            carbon_2019 = (
                self.msci_information["CARBON_EMISSIONS_SCOPE_12_INTEN_FY19"]
                + self.msci_information["CARBON_EMISSIONS_SCOPE_3_FY19"]
                / self.msci_information["SALES_USD_FY19"]
            )
        else:
            carbon_2019 = 0

        if carbon_2019 > 0:
            self.information["Decarb"] = carbon_rn / carbon_2019 - 1
        else:
            self.information["Decarb"] = 0

    def calculate_carbon_intensity(self) -> None:
        """
        Calculate the carbon intensity of a company
        save carbon intensity in information[Carbon Intensity (Scope 123)]

        Calculation
        -----------
            CARBON_EMISSIONS_SCOPE123 / SALES_USD_RECENT
        """
        if (
            self.msci_information["SALES_USD_RECENT"] > 0
            and self.msci_information["CARBON_EMISSIONS_SCOPE123"] > 0
        ):
            carbon_intensity = (
                self.msci_information["CARBON_EMISSIONS_SCOPE123"]
                / self.msci_information["SALES_USD_RECENT"]
            )
        # numerator or denominator are zero
        # --> replace with median of sub industry
        else:
            carbon_intensity = self.information["Sub-Industry"].information[
                "Sub-Sector Median"
            ]

        self.information["Carbon Intensity (Scope 123)"] = carbon_intensity

    def check_theme_requirements(self, themes: dict) -> None:
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
        for theme, theme_obj in themes.items():
            msci_sum = 0
            iss_max = 0
            self.information[theme + "_ISSKeyAdd"] = False
            # sum up MSCI theme specific column to calculate msci revenue
            for sub in theme_obj.msci_sub:
                msci_sum = np.nansum([self.msci_information[sub], msci_sum])
            self.information[theme + "_MSCI"] = msci_sum

            # max over ISS theme specific columns to calculate iss revenue
            for iss in theme_obj.iss_cols:
                iss_max = np.nanmax([iss_max, float(self.sdg_information[iss])])
                # go to Prod column and check for theme specific keywords
                iss_prod = iss.replace("Percent", "Prod")
                description = self.sdg_information[iss_prod]
                if not pd.isna(description):
                    for product_key in theme_obj.product_key_add:
                        if product_key in description:
                            self.information[theme + "_ISSKeyAdd"] = True
            # multiply with 100 to make comparable to MSCI
            iss_max *= 100
            self.information[theme + "_ISS"] = iss_max

            # check if company fulfills theme specific requirements
            func_ = getattr(theme_obj, theme)
            theme_score = func_(
                industry=self.information["Sub-Industry"].class_name,
                iss_key=self.information[theme + "_ISSKeyAdd"],
                msci_rev=msci_sum,
                iss_rev=iss_max,
                capex=self.information["CapEx"],
                palm_tie=self.msci_information["PALM_TIE"],
                orphan_drug_rev=self.msci_information["SI_ORPHAN_DRUGS_REV"],
                acc_to_health=self.msci_information["ACCESS_TO_HLTHCRE_SCORE"],
                trailing_rd_sales=self.rud_information["TRAILING_12M_R&D_%_SALES"],
                social_fin=self.msci_information["SI_SOCIAL_FIN__MAX_REV"],
                social_connect=self.msci_information["SI_CONNECTIVITY_MAX_REV"],
                social_inclusion=self.msci_information["SI_BASIC_N_TOTAL_MAX_REV"],
            )

            # if requirement fulfilled, assign theme to company and vice versa
            if theme_score:
                theme_obj.companies[self.isin] = self
                self.scores["Themes_unadjusted"][theme] = theme_obj
                self.scores["Themes"][theme] = theme_obj
                self.scores["Sustainability_Tag"] = "Y"

                # calculate primary sustainable revenue source
                if msci_sum > current_max:
                    self.information["Primary_Rev_Sustainable"] = theme_obj
                    current_max = msci_sum
                    current_sec = iss_max
                elif msci_sum == current_max and iss_max > current_sec:
                    self.information["Primary_Rev_Sustainable"] = theme_obj
                    current_max = msci_sum
                    current_sec = iss_max
                if iss_max > current_max:
                    self.information["Primary_Rev_Sustainable"] = theme_obj
                    current_max = iss_max
                    current_sec = msci_sum
                elif iss_max == current_max and msci_sum > current_sec:
                    self.information["Primary_Rev_Sustainable"] = theme_obj
                    current_max = iss_max
                    current_sec = msci_sum

    def calculate_esrm_score(self) -> None:
        """
        Calculuate esrm score for each company

        Rules
        -----
        1) For each category save indicator fields and EM and DM flag scorings
        2) For each company:
            2.1) Get ESRM Module (category)
            2.2) Iterate over category indicator fields and
                - count number of flags based on operator and flag threshold
                - save flag value in indicator_Flag
                - create ESRM score based on flag scorings and region
            2.3) Create Governance_Score based on Region_Theme
            2.4) Save flags in company_information
        """

        if self.non_applicable_securities():
            self.scores["ESRM_Flags"] = dict()
            self.scores["Governance_Flags"] = dict()
            self.scores["NA_Flags_ESRM"] = dict()
            self.scores["NA_Flags_Governance"] = dict()
            return

        # get ESRM Module
        df_ = self.information["ESRM Module"].esrm_df
        # count flags
        esrm_flags, na_esrm, esrm_counter = util_functions.check_threshold(
            df_, self.msci_information
        )

        # specific region for company (DM or EM)
        region = self.information["Issuer_Country"].information["Region_EM"]
        # --> regions have different scoring thresholds
        if region == "DM":
            l = self.information["ESRM Module"].DM_flags
        else:
            l = self.information["ESRM Module"].EM_flags

        # create esrm score based on flag scoring
        for i in range(len(l) - 1, -1, -1):
            if esrm_counter >= (l[i]):
                self.scores["ESRM_Score_unadjusted"] = i + 1
                self.scores["ESRM_Score"] = i + 1

                if i + 1 == 5:
                    self.scores["Review_Flag"] = "Needs Review"
                break

        if (
            self.msci_information["UNGC_COMPLIANCE"] == "Fail"
            or self.msci_information["OVERALL_FLAG"] == "Red"
            or self.msci_information["IVA_COMPANY_RATING"] == "CCC"
        ):
            self.scores["ESRM_Score_unadjusted"] = 5
            self.scores["ESRM_Score"] = 5

        # calculate governance score
        df_ = self.information["region_theme"].esrm_df
        gov_flags, na_gov, gov_counter = util_functions.check_threshold(
            df_, self.msci_information
        )

        scoring_d = self.information["region_theme"].EM_flags
        for i in range(len(scoring_d) - 1, -1, -1):
            if gov_counter >= (scoring_d[i]):
                self.scores["Governance_Score_unadjusted"] = i + 1
                self.scores["Governance_Score"] = i + 1

                if i + 1 == 5:
                    self.scores["Review_Flag"] = "Needs Review"
                break

        self.scores["ESRM_Flags"] = esrm_flags
        self.scores["Governance_Flags"] = gov_flags
        self.scores["NA_Flags_ESRM"] = na_esrm
        self.scores["NA_Flags_Governance"] = na_gov

    def non_applicable_securities(self) -> bool:
        """
        For non-applicable securities, apply a score of 0 accross all risk scores

        Rules
        -----
            - Sector Level 2 in excemption list
            - BCLASS is Treasury

        Returns
        -------
        bool
            security is non applicable
        """
        sector_level2 = ["Cash and Other"]

        if self.information["BCLASS_Level4"].class_name == "Treasury":
            return True
        elif self.information["Sector_Level_2"] in sector_level2:
            return True
        elif "TCW" in self.msci_information["ISSUER_NAME"]:
            return True
        elif " ETF " in self.msci_information["ISSUER_NAME"]:
            return True
        elif self.isin[:16] == "MKT VALUE ADJUST":
            return True
        return False

    def calculate_transition_score(self) -> None:
        """
        Calculate transition score (Transition_Score) for each company

        Rules
        -----
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
        sub_sector = self.information["Sub-Industry"]
        transition_score = industry.initial_score + self.scores["Target_Score"]

        # carbon intensity quantile credit
        ci = self.information["Carbon Intensity (Scope 123)"]
        if ci < sub_sector.information["Sub-Sector Q Low"]:
            transition_score -= 2
        elif ci >= sub_sector.information["Sub-Sector Q High"]:
            transition_score -= 0
        else:
            transition_score -= 1

        # assign transition score
        transition_score = np.maximum(transition_score, 1)
        self.scores["Transition_Score_unadjusted"] = transition_score
        self.scores["Transition_Score"] = transition_score

        if transition_score == 5:
            self.scores["Review_Flag"] = "Needs Review"

    def calculate_target_score(self) -> None:
        """
        Calculate the target score of a company
        This target score will be used as reduction in transition score calculation
        if company fulfills one of the rules below, transition score will be reduced by one
        save target score in company_information[Target_Score]

        Rules
        -----
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

    def create_transition_tag(self) -> None:
        """
        create transition tags

        Rules
        -----
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
        if self.information["transition_info"]:
            reduction_target = self.sdg_information["ClimateGHGReductionTargets"]
            sbti_commited_target = self.msci_information["HAS_COMMITTED_TO_SBTI_TARGET"]
            sbti_approved_target = self.msci_information["HAS_SBTI_APPROVED_TARGET"]

            transition_ = transition.calculate_transition_tag(
                climate_rev=self.information["Climate_Revenue"],
                company_capex=self.information["CapEx"],
                company_decarb=self.information["Decarb"],
                reduction_target=reduction_target,
                sbti_approved_target=sbti_approved_target,
                sbti_commited_target=sbti_commited_target,
                **self.information["transition_info"]
            )

            # transition requirements fulfilled
            if transition_:
                self.scores["Transition_Tag"] = "Y"
                self.scores["Transition_Category_unadjusted"].append(
                    self.information["transition_info"]["ACRONYM"]
                )
                self.scores["Transition_Category"].append(
                    self.information["transition_info"]["ACRONYM"]
                )
                return

    def calculate_corporate_score(self) -> None:
        """
        Calculate corporate score for a company based on other scores.

        Calculation
        -----------
            (Governance Score + ESRM Score + Transition Score) / 3
        """
        self.scores["Corporate_Score"] = np.mean(
            [
                self.scores["ESRM_Score"],
                self.scores["Governance_Score"],
                self.scores["Transition_Score"],
            ]
        )

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level

        Rules
        -----
            - if one of esrm, governance or transition score is 5: Poor
            - if corporate score between 1 and 2: Leading
            - if corporate score between 2 and 4: Average
            - if corporate score above 4: Poor
            - if corporate score 0: not scored
        """
        score = self.scores["Corporate_Score"]
        esrm_score = self.scores["ESRM_Score"]
        gov_score = self.scores["Governance_Score"]
        trans_score = self.scores["Transition_Score"]
        for sec, sec_store in self.securities.items():
            if esrm_score == 5 or gov_score == 5 or trans_score == 5:
                sec_store.scores["Risk_Score_Overall"] = "Poor Risk Score"
            else:
                sec_store.set_risk_overall_score(score)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order
        -----
        1) Poor Data
        2) Poor Transition, Governance or ESRM Score
        3) Is Labeled Bond
        4) Analyst Adjustment
        5) Is Sustainable
        6) Is Transition
        7) Is Leading
        8) Is not Scored
        """
        transition_score = self.scores["Transition_Score"]
        governance_score = self.scores["Governance_Score"]
        na_flags_governance = self.scores["NA_Flags_Governance"]
        esrm_score = self.scores["ESRM_Score"]
        na_flags_esrm = self.scores["NA_Flags_ESRM"]
        score_sum = governance_score + transition_score + esrm_score
        transition_tag = self.scores["Transition_Tag"]
        sustainability_tag = self.scores["Sustainability_Tag"]
        for sec, sec_store in self.securities.items():
            labeled_bond_tag = sec_store.information["Labeled ESG Type"]
            sec_store.level_5()

            if governance_score == 5:
                if (
                    sum(na_flags_esrm.values()) >= 7
                    or sum(na_flags_governance.values()) >= 7
                ):
                    sec_store.has_no_data()
                else:
                    sec_store.is_score_5("Governance")

            elif esrm_score == 5:
                if (
                    sum(na_flags_esrm.values()) >= 7
                    or sum(na_flags_governance.values()) >= 7
                ):
                    sec_store.has_no_data()
                else:
                    sec_store.is_score_5("ESRM")

            elif transition_score == 5:
                if (
                    sum(na_flags_esrm.values()) >= 7
                    or sum(na_flags_governance.values()) >= 7
                ):
                    sec_store.has_no_data()
                else:
                    sec_store.is_score_5("Transition")

            elif labeled_bond_tag == "Labeled Green/Sustainable Linked":
                sec_store.is_esg_labeled("Green/Sustainable Linked")
            elif labeled_bond_tag == "Labeled Green":
                sec_store.is_esg_labeled("Green")
            elif labeled_bond_tag == "Labeled Social":
                sec_store.is_esg_labeled("Social")
            elif labeled_bond_tag == "Labeled Sustainable":
                sec_store.is_esg_labeled("Sustainable")
            elif labeled_bond_tag == "Labeled Sustainable Linked":
                sec_store.is_esg_labeled("Sustainability-Linked Bonds")
            elif labeled_bond_tag == "Labeled Sustainable/Sustainable Linked":
                sec_store.is_esg_labeled("Sustainable/Sustainability-Linked Bonds")

            elif sustainability_tag == "Y*":
                sec_store.is_sustainable()

            elif transition_tag == "Y*":
                sec_store.is_transition()

            elif sustainability_tag == "Y":
                sec_store.is_sustainable()

            elif transition_tag == "Y":
                sec_store.is_transition()

            elif score_sum <= 6 and score_sum > 0:
                sec_store.is_leading()

            elif score_sum == 0:
                sec_store.is_not_scored()

    def replace_unassigned_industry(self, industries: dict) -> None:
        """
        Split companies with unassigned industry and sub-industry into
        high and low transition risk
        --> check if carbon intensity is bigger or smaller than predefined threshold

        Parameters
        ----------
        high_threshold: float
            high threshold for transition risk
        industries: dict
            dictionary of all industry objects
        """
        if self.information["Industry"].name == "Unassigned BCLASS":
            # create new Industry objects for Unassigned High and Low
            carb_int = self.information["Carbon Intensity (Scope 123)"]
            # carbon intensity greater than threshold --> high risk
            if (
                carb_int
                >= self.information["Sub-Industry"].information["Sub-Sector Median"]
            ):
                self.information["Transition_Risk_Module"] = "High"
                industries["Unassigned BCLASS High"].companies[self.isin] = self
                self.information["Industry"] = industries["Unassigned BCLASS High"]
            # carbon intensity smaller than threshold --> low risk
            else:
                self.information["Transition_Risk_Module"] = "Low"
                industries["Unassigned BCLASS Low"].companies[self.isin] = self
                self.information["Industry"] = industries["Unassigned BCLASS Low"]

    def iter(
        self,
        companies: dict,
        regions: dict,
        msci_adjustment_dict: dict,
        industries: dict,
        category_d: dict,
        themes: dict,
        transition_company_mapping: dict,
    ) -> None:
        """
        - attach region information
        - attach category
        - attach analyst adjustment
        - run company specific calculations

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
        msci_adjustment_dict: dict
            dictionary of Analyst Adjustments
        industries: dict
            dictionary of all industry objects
        category_d: dict
            dictionary of ESRM categories
        themes: dict
            dictionary of themes with theme as key, corresponding theme object as value
        transition_company_mapping: dict
            companies with company specific improver/enabler tag
        """
        super().iter(
            regions=regions,
            msci_adjustment_dict=msci_adjustment_dict,
            industries=industries,
            category_d=category_d,
            themes=themes,
            transition_company_mapping=transition_company_mapping,
        )

        # calculate capex
        self.calculate_capex()

        # calculate climate revenue
        self.calculate_climate_revenue()

        # calculate decarbonization
        self.calculate_climate_decarb()

        # calculate carbon intensite --> if na, assign industry median
        self.calculate_carbon_intensity()

        # replace industry for Unassigned BCLASS
        self.replace_unassigned_industry(industries)

        # overwrite improver/enabler tag for company specific companies
        self.attach_transition_info(transition_company_mapping)

        # attach region
        self.attach_region(regions)

        # attach category
        self.attach_category(category_d)

        # attach analyst adjustment
        self.attach_analyst_adjustment(msci_adjustment_dict)

        # check for sustainable theme
        self.check_theme_requirements(themes)


class CashStore(CashStore):
    """
    Cash object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        company's isin
    row_data: pd.Series
        msci information
    """

    def __init__(self, isin: str, row_data: Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level

        Rules
        -----
            - cash is not scored
        """
        for sec, sec_store in self.securities.items():
            sec_store.set_risk_overall_score(0)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order
        -----
        1) Analyst Adjustment
        2) Is not Scored
        """
        transition_tag = self.scores["Transition_Tag"]
        sustainability_tag = self.scores["Sustainability_Tag"]
        for sec, sec_store in self.securities.items():
            sec_store.level_5()

            if sustainability_tag == "Y*":
                sec_store.is_sustainable()

            elif transition_tag == "Y*":
                sec_store.is_transition()
            else:
                sec_store.is_not_scored()

    def iter(
        self,
        regions: dict,
        gics_d: dict,
        bclass_d: dict,
        exclusion_dict: dict,
    ) -> None:
        """
        - attach region
        - attach industry
        - attach exclusions

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass sub industries with bclass as key, bclass object as value
        exclusion_dict: dict
            dictionary of Exclusions
        """
        super().iter(
            regions=regions,
            gics_d=gics_d,
            bclass_d=bclass_d,
            exclusion_dict=exclusion_dict,
        )

        # attach region
        self.attach_region(regions)

        # attach industry and sub industry
        self.attach_industry(gics_d, bclass_d)

        # attach exclusion df
        self.attach_exclusion(exclusion_dict)


class MuniStore(MuniStore):
    """
    Muni object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        muni's isin
    """

    def __init__(self, isin: str, row_data: Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level

        Rules
        -----
            - if muni score between 1 and 2: Leading
            - if muni score between 2 and 4: Average
            - if muni score above 4: Poor
            - if muni score 0: not scored
        """
        score = self.scores["Muni_Score"]
        for sec, sec_store in self.securities.items():
            sec_store.set_risk_overall_score(score)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order
        -----
        1) Muni Score is 5
        2) Is Labeled Bond
        3) Analyst Adjustments
        4) Is Sustainable
        5) Is Transition
        6) Is Leading
        7) Is not Scored
        """
        score = self.scores["Muni_Score"]
        transition_tag = self.scores["Transition_Tag"]
        sustainability_tag = self.scores["Sustainability_Tag"]
        for sec, sec_store in self.securities.items():
            labeled_bond_tag = sec_store.information["Labeled ESG Type"]
            sec_store.level_5()

            if score == 5:
                sec_store.is_score_5("Muni")
            elif labeled_bond_tag == "Labeled Green/Sustainable Linked":
                sec_store.is_esg_labeled("Green/Sustainable Linked")
            elif labeled_bond_tag == "Labeled Green":
                sec_store.is_esg_labeled("Green")
            elif labeled_bond_tag == "Labeled Social":
                sec_store.is_esg_labeled("Social")
            elif labeled_bond_tag == "Labeled Sustainable":
                sec_store.is_esg_labeled("Sustainable")
            elif labeled_bond_tag == "Labeled Sustainable Linked":
                sec_store.is_esg_labeled("Sustainability-Linked Bonds")
            elif labeled_bond_tag == "Labeled Sustainable/Sustainable Linked":
                sec_store.is_esg_labeled("Sustainable/Sustainability-Linked Bonds")

            elif sustainability_tag == "Y*":
                sec_store.is_sustainable()

            elif transition_tag == "Y*":
                sec_store.is_transition()

            elif sustainability_tag == "Y":
                sec_store.is_sustainable()

            elif transition_tag == "Y":
                sec_store.is_transition()

            elif score <= 2 and score > 0:
                sec_store.is_leading()

            elif score == 0:
                sec_store.is_not_scored()

    def iter(
        self,
        regions: dict,
        gics_d: dict,
        bclass_d: dict,
        exclusion_dict: dict,
    ) -> None:
        """
        - attach region
        - attach industry
        - attach exclusions

        Parameters
        ----------
        regions: dict
            dictionary of all region
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass sub industries with bclass as key, bclass object as value
        exclusion_dict: dict
            dictionary of Exclusions
        """
        super().iter(
            regions=regions,
            gics_d=gics_d,
            bclass_d=bclass_d,
            exclusion_dict=exclusion_dict,
        )

        # attach region
        self.attach_region(regions)

        # attach industry and sub industry
        self.attach_industry(gics_d, bclass_d)

        # attach exclusion df
        self.attach_exclusion(exclusion_dict)


class SecuritizedStore(SecuritizedStore):
    """
    Securitized object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        securitized's isin
    """

    def __init__(self, isin: str, row_data: Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)

    def calculate_securitized_score(
        self, green: list, social: list, sustainable: list, clo: list
    ) -> None:
        """
        Calculation of Securitized Score (on security level)

        Parameters
        ----------
        green: list
            list of esg collat types considered green
        social: list
            list of esg collat types considered social
        sustainable: list
            list of esg collat types considered sustainable
        clo: list
            list of esg collat types considered clo
        """
        for sec, sec_store in self.securities.items():
            if (
                not pd.isna(sec_store.information["Labeled ESG Type"])
            ) or sec_store.information["Issuer ESG"] == "Yes":
                if pd.isna(sec_store.information["TCW ESG"]):
                    sec_store.scores["Securitized_Score_unadjusted"] = 5
                    sec_store.scores["Securitized_Score"] = 5
                else:
                    sec_store.scores["Securitized_Score_unadjusted"] = 1
                    sec_store.scores["Securitized_Score"] = 1
            elif " TBA " in sec_store.information["Security_Name"]:
                sec_store.scores["Securitized_Score_unadjusted"] = 3
                sec_store.scores["Securitized_Score"] = 3
            elif (
                sec_store.information["ESG Collateral Type"]["ESG Collat Type"]
                == "Low WACI (Q2) Only"
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 3
                sec_store.scores["Securitized_Score"] = 3
            elif sec_store.information["ESG Collateral Type"]["ESG Collat Type"] in clo:
                sec_store.scores["Securitized_Score_unadjusted"] = 2
                sec_store.scores["Securitized_Score"] = 2
            elif (
                "20%" in sec_store.information["ESG Collateral Type"]["ESG Collat Type"]
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 3
                sec_store.scores["Securitized_Score"] = 3
            elif (
                sec_store.information["ESG Collateral Type"]["ESG Collat Type"] in green
                and sec_store.information["Labeled ESG Type"] != "Labeled Green"
                and sec_store.information["TCW ESG"] == "TCW Green"
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 2
                sec_store.scores["Securitized_Score"] = 2
            elif (
                sec_store.information["ESG Collateral Type"]["ESG Collat Type"]
                in social
                and sec_store.information["Labeled ESG Type"] != "Labeled Social"
                and sec_store.information["TCW ESG"] == "TCW Social"
                and not "TBA " in sec_store.information["Security_Name"]
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 2
                sec_store.scores["Securitized_Score"] = 2
            elif (
                sec_store.information["ESG Collateral Type"]["ESG Collat Type"]
                in sustainable
                and sec_store.information["Labeled ESG Type"] != "Labeled Sustainable"
                and sec_store.information["TCW ESG"] == "TCW Sustainable"
                and not "TBA " in sec_store.information["Security_Name"]
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 2
                sec_store.scores["Securitized_Score"] = 2
            elif (
                (pd.isna(sec_store.information["Labeled ESG Type"]))
                and pd.isna(sec_store.information["TCW ESG"])
                and not "TBA " in sec_store.information["Security_Name"]
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 4
                sec_store.scores["Securitized_Score"] = 4

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level

        Rules
        -----
            - if securitized score between 1 and 2: Leading
            - if securitized score between 2 and 4: Average
            - if securitized score above 4: Poor
            - if securitized score 0: not scored
        """
        for sec, sec_store in self.securities.items():
            score = sec_store.scores["Securitized_Score"]
            sec_store.set_risk_overall_score(score)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order
        -----
        1) Securitized Score is 5
        2) Is Labeled Bond
        3) Is CLO
        4) Has ESG Collat Type
        5) Is TBA
        6) Is Leading
        7) Is not scored
        """
        for sec, sec_store in self.securities.items():
            labeled_bond_tag = sec_store.information["Labeled ESG Type"]
            score = sec_store.scores["Securitized_Score"]
            sec_store.level_5()

            if score == 5:
                sec_store.is_score_5("Securitized")

            elif labeled_bond_tag == "Labeled Green/Sustainable Linked":
                sec_store.is_esg_labeled("Green/Sustainable Linked")
            elif labeled_bond_tag == "Labeled Green":
                sec_store.is_esg_labeled("Green")
            elif labeled_bond_tag == "Labeled Social":
                sec_store.is_esg_labeled("Social")
            elif labeled_bond_tag == "Labeled Sustainable":
                sec_store.is_esg_labeled("Sustainable")
            elif labeled_bond_tag == "Labeled Sustainable Linked":
                sec_store.is_esg_labeled("Sustainability-Linked Bonds")
            elif labeled_bond_tag == "Labeled Sustainable/Sustainable Linked":
                sec_store.is_esg_labeled("Sustainable/Sustainability-Linked Bonds")
            elif sec_store.information["ESG Collateral Type"]["G/S/S"] == "CLO":
                sec_store.is_CLO()
            elif (
                not sec_store.information["ESG Collateral Type"]["ESG Collat Type"]
                == "Unknown"
            ):
                sec_store.has_collat_type()

            elif " TBA " in sec_store.information["Security_Name"]:
                sec_store.is_TBA()

            elif score <= 2 and score > 0:
                sec_store.is_leading()
            elif score == 0:
                sec_store.is_not_scored()

    def iter(
        self,
        regions: dict,
        gics_d: dict,
        bclass_d: dict,
        exclusion_dict: dict,
    ) -> None:
        """
        - attach region
        - attach industry
        - attach exclusions

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass sub industries with bclass as key, bclass object as value
        exclusion_dict: dict
            dictionary of Exclusions
        """
        super().iter(
            regions=regions,
            gics_d=gics_d,
            bclass_d=bclass_d,
            exclusion_dict=exclusion_dict,
        )

        # attach region
        self.attach_region(regions)

        # attach industry and sub industry
        self.attach_industry(gics_d, bclass_d)

        # attach exclusion df
        self.attach_exclusion(exclusion_dict)


class SovereignStore(SovereignStore):
    """
    Sovereign object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        company's isin
    """

    def __init__(self, isin: str, row_data: Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)

    def update_sovereign_score(self) -> None:
        """
        Set Sovereign Score
        """
        self.scores["Sovereign_Score_unadjusted"] = deepcopy(
            self.information["Issuer_Country"].information["Sovereign_Score"]
        )
        self.scores["Sovereign_Score"] = self.information["Issuer_Country"].information[
            "Sovereign_Score"
        ]

        if self.msci_information["GOVERNMENT_ESG_RATING"] == "CCC":
            self.scores["Sovereign_Score_unadjusted"] = 5
            self.scores["Sovereign_Score"] = 5

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level:

        Rules
        -----
            - if sovereign score between 1 and 2: Leading
            - if sovereign score between 2 and 4: Average
            - if sovereign score above 4: Poor
            - if sovereign score 0: not scored
        """
        score = self.scores["Sovereign_Score"]
        for sec, sec_store in self.securities.items():
            sec_store.set_risk_overall_score(score)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order:
        1) Is Labeled Bond
        2) Analyst Adjustment
        3) Is Leading
        4) Is not Scored
        5) Sovereign Score is 5
        """
        score = self.scores["Sovereign_Score"]
        transition_tag = self.scores["Transition_Tag"]
        sustainability_tag = self.scores["Sustainability_Tag"]
        for sec, sec_store in self.securities.items():
            labeled_bond_tag = sec_store.information["Labeled ESG Type"]
            sec_store.level_5()

            if labeled_bond_tag == "Labeled Green":
                sec_store.is_esg_labeled("Green")
            elif labeled_bond_tag == "Labeled Green/Sustainable Linked":
                sec_store.is_esg_labeled("Green/Sustainable Linked")
            elif labeled_bond_tag == "Labeled Social":
                sec_store.is_esg_labeled("Social")
            elif labeled_bond_tag == "Labeled Sustainable":
                sec_store.is_esg_labeled("Sustainable")
            elif labeled_bond_tag == "Labeled Sustainable Linked":
                sec_store.is_esg_labeled("Sustainability-Linked Bonds")
            elif labeled_bond_tag == "Labeled Sustainable/Sustainable Linked":
                sec_store.is_esg_labeled("Sustainable/Sustainability-Linked Bonds")

            elif sustainability_tag == "Y*":
                sec_store.is_sustainable()

            elif transition_tag == "Y*":
                sec_store.is_transition()

            elif score <= 2 and score > 0:
                sec_store.is_leading()
            elif score == 0:
                sec_store.is_not_scored()
            elif score == 5:
                sec_store.is_score_5("Sovereign")

    def iter(
        self,
        regions: dict,
        msci_adjustment_dict: dict,
        gics_d: dict,
        bclass_d: dict,
        exclusion_dict: dict,
    ) -> None:
        """
        - attach region information
        - attach analyst adjustment
        - attach exclusions
        - attach industry

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
        msci_adjustment_dict: pd.Dataframe
            dictionary of Analyst Adjustments
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass sub industries with bclass as key, bclass object as value
        exclusion_dict: dict
            dictionary of Exclusions
        """
        super().iter(
            regions=regions,
            msci_adjustment_dict=msci_adjustment_dict,
            gics_d=gics_d,
            bclass_d=bclass_d,
            exclusion_dict=exclusion_dict,
        )

        self.attach_region(regions)
        self.attach_analyst_adjustment(msci_adjustment_dict)
        self.attach_exclusion(exclusion_dict)
        self.attach_industry(gics_d, bclass_d)
