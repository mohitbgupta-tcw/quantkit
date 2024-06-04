import numpy as np
import pandas as pd
from copy import deepcopy
from quantkit.core.characteristics.sector_level2 import (
    SectorLevel2Store,
    CorporateStore,
    CashStore,
    MuniStore,
    SovereignStore,
    SecuritizedStore,
)
import quantkit.utils.util_functions as util_functions
import quantkit.risk_framework.core.transition as transition


class SectorLevel2StoreESG(SectorLevel2Store):
    """
    Sector Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, security, sector_level2: str, **kwargs) -> None:
        self.security = security
        self.sector_level2 = sector_level2

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

        if (
            self.security.issuer_store.information["BCLASS_LEVEL4"].class_name
            == "Treasury"
        ):
            return True
        elif "TCW" in self.security.information["SECURITY_NAME"]:
            return True
        elif " ETF " in self.security.information["SECURITY_NAME"]:
            return True
        elif "MKT VALUE ADJUST" in self.security.information["SECURITY_NAME"]:
            return True
        return False

    def calculate_sustainable_theme(self, themes: dict) -> None:
        """
        assign Sustainability Tag to security

        Parameters
        ----------
        themes: dict
            dictionary of themes with theme as key, corresponding theme object as value
        """
        pass

    def calculate_transition_score(self) -> None:
        """
        Calculate transition score (Transition_Score) for security
        """
        pass

    def calculate_esrm_score(self) -> None:
        """
        Calculuate esrm score for security
        """
        pass

    def calculate_sovereign_score(self) -> None:
        """
        Set Sovereign Score
        """
        pass

    def calculate_security_score(self) -> None:
        """
        Calculate corporate score for a security
        """
        pass


class CorporateStore(CorporateStore, SectorLevel2StoreESG):
    """
    Corporate Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def calculate_sustainable_theme(self, themes: dict) -> None:
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
            self.security.issuer_store.esg_information[theme + "_ISSKeyAdd"] = False
            # sum up MSCI theme specific column to calculate msci revenue
            for sub in theme_obj.msci_sub:
                msci_sum = np.nansum(
                    [self.security.issuer_store.msci_information[sub], msci_sum]
                )
            self.security.issuer_store.esg_information[theme + "_MSCI"] = msci_sum

            # max over ISS theme specific columns to calculate iss revenue
            for iss in theme_obj.iss_cols:
                iss_max = np.nanmax(
                    [iss_max, float(self.security.issuer_store.sdg_information[iss])]
                )
                # go to Prod column and check for theme specific keywords
                iss_prod = iss.replace("Percent", "Prod")
                description = self.security.issuer_store.sdg_information[iss_prod]
                if not pd.isna(description):
                    for product_key in theme_obj.product_key_add:
                        if product_key in description:
                            self.security.issuer_store.esg_information[
                                theme + "_ISSKeyAdd"
                            ] = True
            # multiply with 100 to make comparable to MSCI
            iss_max *= 100
            self.security.issuer_store.esg_information[theme + "_ISS"] = iss_max

            # check if company fulfills theme specific requirements
            func_ = getattr(theme_obj, theme)
            theme_score = func_(
                industry=self.security.issuer_store.information[
                    "Sub-Industry"
                ].class_name,
                iss_key=self.security.issuer_store.esg_information[
                    theme + "_ISSKeyAdd"
                ],
                msci_rev=msci_sum,
                iss_rev=iss_max,
                capex=self.security.issuer_store.esg_information["CapEx"],
                palm_tie=self.security.issuer_store.msci_information["PALM_TIE"],
                orphan_drug_rev=self.security.issuer_store.msci_information[
                    "SI_ORPHAN_DRUGS_REV"
                ],
                acc_to_health=self.security.issuer_store.msci_information[
                    "ACCESS_TO_HLTHCRE_SCORE"
                ],
                trailing_rd_sales=self.security.issuer_store.rud_information[
                    "TRAILING_12M_R&D_%_SALES"
                ],
                social_fin=self.security.issuer_store.msci_information[
                    "SI_SOCIAL_FIN__MAX_REV"
                ],
                social_connect=self.security.issuer_store.msci_information[
                    "SI_CONNECTIVITY_MAX_REV"
                ],
                social_inclusion=self.security.issuer_store.msci_information[
                    "SI_BASIC_N_TOTAL_MAX_REV"
                ],
            )

            # if requirement fulfilled, assign theme to company and vice versa
            if theme_score:
                theme_obj.issuers[self.security.key] = self.security
                self.security.scores["Themes_unadjusted"][theme] = theme_obj
                self.security.scores["Themes"][theme] = theme_obj
                self.security.scores["Sustainability_Tag"] = "Y"

                # calculate primary sustainable revenue source
                if msci_sum > current_max:
                    self.security.scores["Primary_Rev_Sustainable"] = theme_obj
                    current_max = msci_sum
                    current_sec = iss_max
                elif msci_sum == current_max and iss_max > current_sec:
                    self.security.scores["Primary_Rev_Sustainable"] = theme_obj
                    current_max = msci_sum
                    current_sec = iss_max
                if iss_max > current_max:
                    self.security.scores["Primary_Rev_Sustainable"] = theme_obj
                    current_max = iss_max
                    current_sec = msci_sum
                elif iss_max == current_max and msci_sum > current_sec:
                    self.security.scores["Primary_Rev_Sustainable"] = theme_obj
                    current_max = iss_max
                    current_sec = msci_sum

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
        if self.security.issuer_store.sdg_information["ClimateGHGReductionTargets"] in [
            "Ambitious Target",
            "Approved SBT",
            "Committed SBT",
        ]:
            target_score = -1
        elif self.security.issuer_store.esg_information["CapEx"] >= 30:
            target_score = -1
        elif (
            self.security.issuer_store.msci_information["HAS_COMMITTED_TO_SBTI_TARGET"]
            == 1
        ):
            target_score = -1
        elif (
            self.security.issuer_store.msci_information["HAS_SBTI_APPROVED_TARGET"] == 1
        ):
            target_score = -1
        else:
            target_score = 0
        self.security.scores["Target_Score"] = target_score

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
            self.security.issuer_store.information["Sub-Industry"].class_name
            in Remove_Transition_Set
            and self.security.scores["Sustainability_Tag"] == "Y"
        ):
            return

        # check if company's sub industry has transition plan
        reduction_target = self.security.issuer_store.sdg_information[
            "ClimateGHGReductionTargets"
        ]
        sbti_commited_target = self.security.issuer_store.msci_information[
            "HAS_COMMITTED_TO_SBTI_TARGET"
        ]
        sbti_approved_target = self.security.issuer_store.msci_information[
            "HAS_SBTI_APPROVED_TARGET"
        ]

        transition_ = transition.calculate_transition_tag(
            climate_rev=self.security.issuer_store.esg_information["Climate_Revenue"],
            company_capex=self.security.issuer_store.esg_information["CapEx"],
            company_decarb=self.security.issuer_store.esg_information["Decarb"],
            reduction_target=reduction_target,
            sbti_approved_target=sbti_approved_target,
            sbti_commited_target=sbti_commited_target,
            **self.security.issuer_store.transition_information
        )

        # transition requirements fulfilled
        if transition_:
            self.security.scores["Transition_Tag"] = "Y"
            self.security.scores["Transition_Category_unadjusted"].append(
                self.security.issuer_store.transition_information["ACRONYM"]
            )
            self.security.scores["Transition_Category"].append(
                self.security.issuer_store.transition_information["ACRONYM"]
            )
            return

    def calculate_transition_score(self) -> None:
        """
        Calculate transition score (Transition_Score) for security

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
        industry = self.security.issuer_store.information["Industry"]
        sub_sector = self.security.issuer_store.information["Sub-Industry"]
        transition_score = industry.initial_score + self.security.scores["Target_Score"]

        # carbon intensity quantile credit
        ci = self.security.issuer_store.esg_information["Carbon Intensity (Scope 123)"]
        if ci < sub_sector.information["Sub-Sector Q Low"]:
            transition_score -= 2
        elif ci >= sub_sector.information["Sub-Sector Q High"]:
            transition_score -= 0
        else:
            transition_score -= 1

        # assign transition score
        transition_score = np.maximum(transition_score, 1)
        self.security.scores["Transition_Score_unadjusted"] = transition_score
        self.security.scores["Transition_Score"] = transition_score

        if transition_score == 5:
            self.security.scores["Review_Flag"] = "Needs Review"

    def calculate_sovereign_score(self) -> None:
        """
        For Treasuries update the Sovereign Score (in self.scores)
        """
        if self.security.information["BCLASS_LEVEL4"].class_name == "Treasury":
            self.security.scores["Sovereign_Score_unadjusted"] = deepcopy(
                self.security.information["Issuer_Country"].information[
                    "Sovereign_Score"
                ]
            )
            self.security.scores["Sovereign_Score"] = self.security.information[
                "Issuer_Country"
            ].information["Sovereign_Score"]

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
            return

        # get ESRM Module
        df_ = self.security.issuer_store.esg_information["ESRM Module"].esrm_df
        # count flags
        esrm_flags, na_esrm, esrm_counter = util_functions.check_threshold(
            df_, self.security.issuer_store.msci_information
        )

        # specific region for company (DM or EM)
        region = self.security.information["Issuer_Country"].information["Region_EM"]
        # --> regions have different scoring thresholds
        if region == "DM":
            l = self.security.issuer_store.esg_information["ESRM Module"].DM_flags
        else:
            l = self.security.issuer_store.information["ESRM Module"].EM_flags

        # create esrm score based on flag scoring
        for i in range(len(l) - 1, -1, -1):
            if esrm_counter >= (l[i]):
                self.security.scores["ESRM_Score_unadjusted"] = i + 1
                self.security.scores["ESRM_Score"] = i + 1

                if i + 1 == 5:
                    self.security.scores["Review_Flag"] = "Needs Review"
                break

        if (
            self.security.issuer_store.msci_information["UNGC_COMPLIANCE"] == "Fail"
            or self.security.issuer_store.msci_information["OVERALL_FLAG"] == "Red"
            or self.security.issuer_store.msci_information["IVA_COMPANY_RATING"]
            == "CCC"
        ):
            self.security.scores["ESRM_Score_unadjusted"] = 5
            self.security.scores["ESRM_Score"] = 5

        # calculate governance score
        df_ = self.security.issuer_store.esg_information["region_theme"].esrm_df
        gov_flags, na_gov, gov_counter = util_functions.check_threshold(
            df_, self.security.issuer_store.msci_information
        )

        scoring_d = self.security.issuer_store.information["region_theme"].EM_flags
        for i in range(len(scoring_d) - 1, -1, -1):
            if gov_counter >= (scoring_d[i]):
                self.security.scores["Governance_Score_unadjusted"] = i + 1
                self.security.scores["Governance_Score"] = i + 1

                if i + 1 == 5:
                    self.security.scores["Review_Flag"] = "Needs Review"
                break

        self.security.scores["ESRM_Flags"] = esrm_flags
        self.security.scores["Governance_Flags"] = gov_flags
        self.security.scores["NA_Flags_ESRM"] = na_esrm
        self.security.scores["NA_Flags_Governance"] = na_gov

    def calculate_security_score(self) -> None:
        """
        Calculate corporate score for a security based on other scores.

        Calculation
        -----------
            (Governance Score + ESRM Score + Transition Score) / 3
        """
        self.security.scores["Corporate_Score"] = np.mean(
            [
                self.security.scores["ESRM_Score"],
                self.security.scores["Governance_Score"],
                self.security.scores["Transition_Score"],
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
        score = self.security.scores["Corporate_Score"]
        esrm_score = self.security.scores["ESRM_Score"]
        gov_score = self.security.scores["Governance_Score"]
        trans_score = self.security.scores["Transition_Score"]
        if esrm_score == 5 or gov_score == 5 or trans_score == 5:
            self.security.scores["Risk_Score_Overall"] = "Poor Risk Score"
        else:
            self.security.set_risk_overall_score(score)

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

    def iter(
        self,
        category_d: dict,
    ) -> None:
        """
        - attach region information
        - attach category

        Parameters
        ----------
        category_d: dict
            dictionary of ESRM categories
        """
        super().iter(
            category_d=category_d,
        )

        # attach category
        self.attach_category(category_d)


class CashStore(CashStore, SectorLevel2StoreESG):
    """
    Cash Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level

        Rules
        -----
            - cash is not scored
        """
        self.security.set_risk_overall_score(0)

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


class MuniStore(MuniStore, SectorLevel2StoreESG):
    """
    Muni Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

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
        score = self.security.scores["Muni_Score"]
        self.security.set_risk_overall_score(score)

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


class SecuritizedStore(SecuritizedStore, SectorLevel2StoreESG):
    """
    Muni Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def calculate_security_score(
        self,
    ) -> None:
        """
        Calculation of Securitized Score (on security level)
        """
        if (
            not pd.isna(self.security.information["LABELED_ESG_TYPE"])
        ) or self.security.information["ISSUER_ESG"] == "Yes":
            if pd.isna(self.security.information["TCW_ESG"]):
                self.security.scores["Securitized_Score_unadjusted"] = 5
                self.security.scores["Securitized_Score"] = 5
            else:
                self.security.scores["Securitized_Score_unadjusted"] = 1
                self.security.scores["Securitized_Score"] = 1
        elif " TBA " in self.security.information["SECURITY_NAME"]:
            self.security.scores["Securitized_Score_unadjusted"] = 3
            self.security.scores["Securitized_Score"] = 3
        elif (
            self.security.information["ESG_COLLATERAL_TYPE"]["ESG Collat Type"]
            == "Low WACI (Q2) Only"
        ):
            self.security.scores["Securitized_Score_unadjusted"] = 3
            self.security.scores["Securitized_Score"] = 3
        elif self.security.information["ESG_COLLATERAL_TYPE"]["G/S/S"] == "CLO":
            self.security.scores["Securitized_Score_unadjusted"] = 2
            self.security.scores["Securitized_Score"] = 2
        elif (
            "20%" in self.security.information["ESG_COLLATERAL_TYPE"]["ESG Collat Type"]
        ):
            self.security.scores["Securitized_Score_unadjusted"] = 3
            self.security.scores["Securitized_Score"] = 3
        elif (
            self.security.information["ESG_COLLATERAL_TYPE"]["G/S/S"] == "Green"
            and self.security.information["LABELED_ESG_TYPE"] != "Labeled Green"
            and self.security.information["TCW_ESG"] == "TCW Green"
        ):
            self.security.scores["Securitized_Score_unadjusted"] = 2
            self.security.scores["Securitized_Score"] = 2
        elif (
            self.security.information["ESG_COLLATERAL_TYPE"]["G/S/S"] == "Social"
            and self.security.information["LABELED_ESG_TYPE"] != "Labeled Social"
            and self.security.information["TCW_ESG"] == "TCW Social"
            and not "TBA " in self.security.information["SECURITY_NAME"]
        ):
            self.security.scores["Securitized_Score_unadjusted"] = 2
            self.security.scores["Securitized_Score"] = 2
        elif (
            self.security.information["ESG_COLLATERAL_TYPE"]["G/S/S"] == "Sustainable"
            and self.security.information["LABELED_ESG_TYPE"] != "Labeled Sustainable"
            and self.security.information["TCW_ESG"] == "TCW Sustainable"
            and not "TBA " in self.security.information["SECURITY_NAME"]
        ):
            self.security.scores["Securitized_Score_unadjusted"] = 2
            self.security.scores["Securitized_Score"] = 2
        elif (
            (pd.isna(self.security.information["LABELED_ESG_TYPE"]))
            and pd.isna(self.security.information["TCW_ESG"])
            and not "TBA " in self.security.information["SECURITY_NAME"]
        ):
            self.security.scores["Securitized_Score_unadjusted"] = 4
            self.security.scores["Securitized_Score"] = 4

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
        score = self.security.scores["Securitized_Score"]
        self.security.set_risk_overall_score(score)

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


class SovereignStore(SovereignStore, SectorLevel2StoreESG):
    """
    Sovereign Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def calculate_sovereign_score(self) -> None:
        """
        Set Sovereign Score
        """
        self.security.scores["Sovereign_Score_unadjusted"] = deepcopy(
            self.security.information["Issuer_Country"].information["Sovereign_Score"]
        )
        self.security.scores["Sovereign_Score"] = self.security.information[
            "Issuer_Country"
        ].information["Sovereign_Score"]

        if (
            self.security.issuer_store.msci_information["GOVERNMENT_ESG_RATING"]
            == "CCC"
        ):
            self.security.scores["Sovereign_Score_unadjusted"] = 5
            self.security.scores["Sovereign_Score"] = 5

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
        score = self.security.scores["Sovereign_Score"]
        self.security.set_risk_overall_score(score)

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
