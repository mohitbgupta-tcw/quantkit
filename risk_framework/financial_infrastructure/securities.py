import quantkit.core.financial_infrastructure.securities as securities
import quantkit.risk_framework.core.adjustment as adjustment
from typing import Union
import numpy as np
import pandas as pd


class SecurityStore(securities.SecurityStore):
    """
    Security Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors
        - security information

    Parameters
    ----------
    key: str
        Security's key
    information: dict
        dictionary of security specific information
    """

    def __init__(self, key: str, information: dict, **kwargs) -> None:
        super().__init__(key, information, **kwargs)

        self.scores = dict(
            Securitized_Score=0,
            Securitized_Score_unadjusted=0,
            Risk_Score_Overall="Poor Risk Score",
            Themes=dict(),
            Themes_unadjusted=dict(),
            Transition_Category=list(),
            Transition_Category_unadjusted=list(),
            Sustainability_Tag="N",
            Transition_Tag="N",
            Muni_Score=0,
            Muni_Score_unadjusted=0,
            Sovereign_Score=0,
            Sovereign_Score_unadjusted=0,
            ESRM_Score=0,
            ESRM_Score_unadjusted=0,
            Governance_Score=0,
            Governance_Score_unadjusted=0,
            Target_Score=0,
            Transition_Score=0,
            Transition_Score_unadjusted=0,
            Corporate_Score=0,
            Review_Flag="",
            Review_Comments="",
            ESRM_Flags=dict(),
            Governance_Flags=dict(),
            NA_Flags_ESRM=dict(),
            NA_Flags_Governance=dict(),
            SClass_Level1="Eligible",
            SClass_Level2="ESG Scores",
            SClass_Level3="ESG Scores",
            SClass_Level4="Average ESG Score",
            SClass_Level4_P="Average ESG Score",
            SClass_Level5="Unknown",
        )

    def add_collateral_type(self, securitized_mapping: dict) -> None:
        """
        Add ESG Collateral Type object to information

        Parameters
        ----------
        securitized_mapping: dict
            mapping for ESG Collat Type
        """
        self.information["ESG_COLLATERAL_TYPE"] = securitized_mapping[
            self.information["ESG_COLLATERAL_TYPE"]
        ]

    def attach_region(self, regions: dict) -> None:
        """
        Attach region information (including ISO2, name, sovereign score) to parent object
        Save region object in self.information["Issuer_Country"]

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
        """
        country = self.information["ISO2"]
        self.information["ISO2"] = regions[country]
        regions[country].add_company(self.key, self)

    def attach_category(self, category_d: dict) -> None:
        """
        Attach ESRM category based on ESRM module of sub industry
        Attach Region Theme based on Region

        Parameters
        ----------
        category_d: dict
            dictionary of ESRM categories
        """
        if not category_d:
            return
        esrm_module = self.information["Sub-Industry"].information["ESRM Module"]
        self.information["ESRM Module"] = category_d[esrm_module]

        # get region theme (DM, JP, EM, EUCohort)
        region_theme = self.information["Issuer_Country"].information["Region"]
        self.information["region_theme"] = category_d[region_theme]

    def set_risk_overall_score(self, score: Union[float, int]) -> None:
        """
        Set Risk Overall score based on type specific score

        Parameters
        ----------
        score: float | int
            type specific score
        """

        if score >= 1 and score <= 2:
            self.scores["Risk_Score_Overall"] = "Leading ESG Score"
        elif score > 2 and score <= 4:
            self.scores["Risk_Score_Overall"] = "Average ESG Score"
        elif score == 0:
            self.scores["Risk_Score_Overall"] = "Not Scored"
        elif score == 5:
            self.scores["Risk_Score_Overall"] = "Poor Risk Score"

    def level_5(self) -> None:
        """
        Set SClass Level 5 as ESG Collat Type
        """
        self.information["SClass_Level5"] = self.information["ESG Collateral Type"][
            "ESG Collat Type"
        ]

    def is_score_5(self, parent_type: str) -> None:
        """
        Set SClass Levels for Poor Scores and Exclude Security.

        Parameters
        ----------
        parent_type: str
            type of parent. Either Muni, Sovereign, Securitized or Corporate
        """
        self.information["SClass_Level4-P"] = f"Poor {parent_type} Score"
        self.information["SClass_Level4"] = f"Poor {parent_type} Score"
        self.information["SClass_Level3"] = "Poor Risk Score"
        self.information["SClass_Level2"] = "Poor Risk Score"
        self.information["SClass_Level1"] = "Excluded"

    def has_no_data(self) -> None:
        """
        Set SClass Levels for Poor Data and Exclude Security.

        Parameters
        ----------
        parent_type: str
            type of parent. Either Muni, Sovereign, Securitized or Corporate
        """
        self.information["SClass_Level4-P"] = f"Poor Data"
        self.information["SClass_Level4"] = f"Poor Data"
        self.information["SClass_Level3"] = "Poor Data"
        self.information["SClass_Level2"] = "Poor Data"
        self.information["SClass_Level1"] = "Excluded"

    def is_esg_labeled(self, labeled_esg_type: str) -> None:
        """
        Set SClass Levels for labeled ESG type bonds and Prefer security.

        Parameters
        ----------
        labeled_esg_type: str
            Type of green bond
        """
        self.information["SClass_Level4-P"] = labeled_esg_type
        self.information["SClass_Level4"] = labeled_esg_type
        self.information["SClass_Level3"] = "ESG-Labeled Bonds"
        self.information["SClass_Level2"] = "ESG-Labeled Bonds"
        self.information["SClass_Level1"] = "Preferred"

    def is_leading(self) -> None:
        """
        Set SClass Levels for leading ESG parents (low type specific score)
        """
        self.information["SClass_Level4"] = "Leading ESG Score"
        self.information["SClass_Level4-P"] = "Leading ESG Score"

    def is_not_scored(self) -> None:
        """
        Set SClass Levels for not scored parents (type specific score equals 0)
        """
        self.information["SClass_Level4"] = "Not Scored"
        self.information["SClass_Level4-P"] = "Not Scored"
        self.information["SClass_Level3"] = "Not Scored"
        self.information["SClass_Level2"] = "Not Scored"

    def is_TBA(self) -> None:
        """
        Set SClass Levels for TBA's to AFFORDABLE
        """
        self.information["SClass_Level4-P"] = "AFFORDABLE"
        self.information["SClass_Level4"] = "AFFORDABLE"
        self.information["SClass_Level3"] = "People"
        self.information["SClass_Level2"] = "Sustainable Theme"
        self.information["SClass_Level1"] = "Preferred"

    def is_CLO(self) -> None:
        """
        Set SClass Levels for CLO's to Leading
        """
        self.information["SClass_Level4-P"] = "Leading ESG Score"
        self.information["SClass_Level4"] = "Leading ESG Score"

    def has_collat_type(self) -> None:
        """
        Set SClass Levels for securities with collat type
        """
        self.information["SClass_Level4-P"] = self.information["ESG Collateral Type"][
            "Primary"
        ]
        self.information["SClass_Level4"] = self.information["ESG Collateral Type"][
            "Primary"
        ]
        self.information["SClass_Level3"] = self.information["ESG Collateral Type"][
            "Sclass_Level3"
        ]
        self.information["SClass_Level2"] = "Sustainable Theme"
        self.information["SClass_Level1"] = "Preferred"

    def is_sustainable(self) -> None:
        """
        Set SClass Levels for sustainable parents (have assigned theme to them)
        """
        sustainability_category = self.issuer_store.scores["Themes"]
        themes = list(sustainability_category.keys())
        self.information["SClass_Level2"] = "Sustainable Theme"
        self.information["SClass_Level1"] = "Preferred"
        if len(sustainability_category) == 1:
            theme = themes[0]
            self.information["SClass_Level4"] = theme
            self.information["SClass_Level4-P"] = theme
            self.information["SClass_Level3"] = sustainability_category[theme].pillar
        elif len(sustainability_category) >= 2:
            self.information["SClass_Level3"] = "Multi-Thematic"
            self.information["SClass_Level4-P"] = self.issuer_store.information[
                "Primary_Rev_Sustainable"
            ].acronym
            people_count = 0
            planet_count = 0
            for t in sustainability_category:
                if sustainability_category[t].pillar == "People":
                    people_count += 1
                elif sustainability_category[t].pillar == "Planet":
                    planet_count += 1

                if planet_count >= 1 and people_count >= 1:
                    self.information["SClass_Level4"] = "Planet & People"
                elif planet_count >= 2:
                    self.information["SClass_Level4"] = "Planet"
                elif people_count >= 2:
                    self.information["SClass_Level4"] = "People"
        else:
            raise ValueError("If sustainability tag is 'Y', theme should be assigned.")

    def is_transition(self) -> None:
        """
        Set SClass Levels for transition parents (have assigned transition theme to them)
        """
        transition_category = self.issuer_store.scores["Transition_Category"]
        self.information["SClass_Level3"] = "Transition"
        self.information["SClass_Level2"] = "Transition"
        self.information["SClass_Level1"] = "Eligible"
        if len(transition_category) > 1:
            self.information["SClass_Level4"] = "Multi-Thematic"
            self.information["SClass_Level4-P"] = transition_category[-1]
        elif len(transition_category) == 1:
            self.information["SClass_Level4"] = transition_category[0]
            self.information["SClass_Level4-P"] = transition_category[0]
        else:
            raise ValueError("If transition tag is 'Y', category should be assigned.")

    def iter_analyst_adjustment(self, themes: dict) -> None:
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
        for adj in self.issuer_store.Adjustment:
            thematic_type = adj["Thematic Type"]
            cat = adj["Category"]
            a = adj["Adjustment"]
            comment = adj["Comments"]
            func_ = getattr(adjustment, thematic_type)
            func_(
                store=self,
                adjustment=a,
                themes=themes,
                theme=cat,
                comment=comment,
            )

    def calculate_sustainable_theme(self, themes: dict) -> None:
        """
        assign Sustainability Tag to security

        Parameters
        ----------
        themes: dict
            dictionary of themes with theme as key, corresponding theme object as value
        """
        self.information["SECTOR_LEVEL_2"].calculate_sustainable_theme(themes)

    def calculate_transition_score(self) -> None:
        """
        Calculate transition score (Transition_Score) for security
        """
        self.information["SECTOR_LEVEL_2"].calculate_transition_score()

    def calculate_sovereign_score(self) -> None:
        """
        Calculate sovereign score for security
        """
        self.information["SECTOR_LEVEL_2"].calculate_sovereign_score()

    def calculate_esrm_score(self) -> None:
        """
        Calculuate esrm score for security
        """
        self.information["SECTOR_LEVEL_2"].calculate_esrm_score()

    def calculate_security_score(self) -> None:
        """
        Calculuate security specific score for security
        """
        self.information["SECTOR_LEVEL_2"].calculate_security_score()
