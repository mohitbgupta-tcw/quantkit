from pandas import Series
from copy import deepcopy
import quantkit.core.financial_infrastructure.sovereigns as sovereigns
import quantkit.risk_framework.financial_infrastructure.asset_base as asset_base


class SovereignStore(asset_base.AssetBase, sovereigns.SovereignStore):
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
        - attach GICS information
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
        self.attach_gics(gics_d)
        self.attach_exclusion(exclusion_dict)
        self.attach_industry(gics_d, bclass_d)
