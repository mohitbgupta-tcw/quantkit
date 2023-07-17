import pandas as pd
import numpy as np
import quantkit.finance.companies.headstore as headstore


class MuniStore(headstore.HeadStore):
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

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level:
            - if muni score between 1 and 2: Leading
            - if muni score between 2 and 4: Average
            - if muni score above 4: Poor
            - if muni score 0: not scored
        """
        score = self.scores["Muni_Score"]
        for s in self.securities:
            self.securities[s].set_risk_overall_score(score)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order:
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
        for s in self.securities:
            self.securities[s].level_5()

            if score == 5:
                self.securities[s].is_score_5("Muni")
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Green/Sustainable Linked"
            ):
                self.securities[s].is_esg_labeled("Green/Sustainable Linked")
            elif self.securities[s].information["Labeled_ESG_Type"] == "Labeled Green":
                self.securities[s].is_esg_labeled("Green")
            elif self.securities[s].information["Labeled_ESG_Type"] == "Labeled Social":
                self.securities[s].is_esg_labeled("Social")
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable"
            ):
                self.securities[s].is_esg_labeled("Sustainable")
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Sustainable Linked"
            ):
                self.securities[s].is_esg_labeled("Sustainability-Linked Bonds")

            elif sustainability_tag == "Y*":
                self.securities[s].is_sustainable()

            elif transition_tag == "Y*":
                self.securities[s].is_transition()

            elif sustainability_tag == "Y":
                self.securities[s].is_sustainable()

            elif transition_tag == "Y":
                self.securities[s].is_transition()

            elif score <= 2 and score > 0:
                self.securities[s].is_leading()

            elif score == 0:
                self.securities[s].is_not_scored()

    def iter(self, regions_df: pd.DataFrame, regions: dict, gics_d: dict) -> None:
        """
        - attach GICS information
        - attach exclusions
        - attach region

        Parameters
        ----------
        regions_df: pd.DataFrame
            DataFrame of regions information
        regions: dict
            dictionary of all region objects
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        # attach GICS
        self.attach_gics(gics_d)

        # attach exclusions
        self.iter_exclusion()

        # attach region
        self.attach_region(regions_df, regions)
