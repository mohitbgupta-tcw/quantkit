import pandas as pd
import numpy as np
import quantkit.finance.companies.headstore as headstore


class SovereignStore(headstore.HeadStore):
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
    """

    def __init__(self, isin: str, **kwargs):
        super().__init__(isin, **kwargs)
        self.msci_information = {}
        self.type = "sovereign"

    def update_sovereign_score(self) -> None:
        """
        Set Sovereign Score
        """
        self.scores["Sovereign_Score"] = self.information["Issuer_Country"].information[
            "Sovereign_Score"
        ]

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level:
            - if sovereign score between 1 and 2: Leading
            - if sovereign score between 2 and 4: Average
            - if sovereign score above 4: Poor
            - if sovereign score 0: not scored
        """
        score = self.scores["Sovereign_Score"]
        for s in self.securities:
            self.securities[s].set_risk_overall_score(score)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order:
        1) Is Labeled Bond
        2) Is Leading
        3) Is not Scored
        4) Sovereign Score is 5
        """
        score = self.scores["Sovereign_Score"]
        for s in self.securities:
            self.securities[s].level_5()

            if self.securities[s].information["Labeled_ESG_Type"] == "Labeled Green":
                self.securities[s].is_esg_labeled("Green")
            elif (
                self.securities[s].information["Labeled_ESG_Type"]
                == "Labeled Green/Sustainable Linked"
            ):
                self.securities[s].is_esg_labeled("Green/Sustainable Linked")
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

            elif score <= 2 and score > 0:
                self.securities[s].is_leading()
            elif score == 0:
                self.securities[s].is_not_scored()
            elif score == 5:
                self.securities[s].is_score_5("Sovereign")
        return

    def iter(
        self,
        regions_df: pd.DataFrame,
        regions: dict,
        adjustment_df: pd.DataFrame,
        gics_d: dict,
    ) -> None:
        """
        - attach region information
        - calculate sovereign score
        - attach analyst adjustment
        - attach GICS information
        - attach exclusions

        Parameters
        ----------
        regions_df: pd.DataFrame
            DataFrame of regions information
        regions: dict
            dictionary of all region objects
        adjustment_df: pd.Dataframe
            DataFrame of Analyst Adjustments
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        self.attach_region(regions_df, regions)
        self.update_sovereign_score()
        self.attach_analyst_adjustment(adjustment_df)
        self.attach_gics(gics_d, self.msci_information["GICS_SUB_IND"])
        self.iter_exclusion()
        return
