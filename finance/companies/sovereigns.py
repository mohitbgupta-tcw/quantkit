import pandas as pd
from copy import deepcopy
import quantkit.finance.companies.headstore as headstore


class SovereignStore(headstore.HeadStore):
    """
    Sovereign object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        company's isin
    """

    def __init__(self, isin: str, **kwargs) -> None:
        super().__init__(isin, **kwargs)
        self.msci_information = {}
        self.type = "sovereign"

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
        regions_df: pd.DataFrame,
        regions: dict,
        msci_adjustment_dict: dict,
        gics_d: dict,
        bclass_d: dict,
    ) -> None:
        """
        - attach region information
        - calculate sovereign score
        - attach analyst adjustment
        - attach GICS information
        - attach exclusions
        - attach industry

        Parameters
        ----------
        regions_df: pd.DataFrame
            DataFrame of regions information
        regions: dict
            dictionary of all region objects
        msci_adjustment_dict: pd.Dataframe
            dictionary of Analyst Adjustments
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass sub industries with bclass as key, bclass object as value
        """
        self.attach_region(regions_df, regions)
        self.update_sovereign_score()
        self.attach_analyst_adjustment(msci_adjustment_dict)
        self.attach_gics(gics_d, self.msci_information["GICS_SUB_IND"])
        self.iter_exclusion()
        self.attach_industry(gics_d, bclass_d)
