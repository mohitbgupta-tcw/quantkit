import pandas as pd
import quantkit.finance.companies.headstore as headstore


class MuniStore(headstore.HeadStore):
    """
    Muni object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        muni's isin
    """

    def __init__(self, isin: str, row_data: pd.Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)
        self.type = "muni"

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

    def iter(self, regions: dict, gics_d: dict, bclass_d: dict) -> None:
        """
        - attach GICS information
        - attach region
        - objectsattach industry

        Parameters
        ----------
        regions: dict
            dictionary of all region
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass sub industries with bclass as key, bclass object as value
        """
        # attach GICS
        self.attach_gics(gics_d)

        # attach region
        self.attach_region(regions)

        # attach industry and sub industry
        self.attach_industry(gics_d, bclass_d)
