import pandas as pd
import numpy as np
import quantkit.finance.companies.headstore as headstore


class SecuritizedStore(headstore.HeadStore):
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

    def calculate_securitized_score(self) -> None:
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

            if " TBA " in sec_store.information["IssuerName"]:
                sec_store.scores["Securitized_Score_unadjusted"] = 3
                sec_store.scores["Securitized_Score"] = 3
            elif sec_store.information["Labeled_ESG_Type"] == "ESG CLO":
                sec_store.scores["Securitized_Score_unadjusted"] = 2
                sec_store.scores["Securitized_Score"] = 2
            elif (
                sec_store.information["ESG_Collateral_Type"]["ESG Collat Type"]
                == "Affordable Multifamily (min 20% aff. units)"
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 3
                sec_store.scores["Securitized_Score"] = 3
            elif (
                not pd.isna(sec_store.information["Labeled_ESG_Type"])
            ) or sec_store.information["Issuer_ESG"] == "Yes":
                if pd.isna(sec_store.information["TCW_ESG"]):
                    sec_store.scores["Securitized_Score_unadjusted"] = 5
                    sec_store.scores["Securitized_Score"] = 5
                else:
                    sec_store.scores["Securitized_Score_unadjusted"] = 1
                    sec_store.scores["Securitized_Score"] = 1
            elif (
                sec_store.information["ESG_Collateral_Type"]["ESG Collat Type"]
                in collat_type_1
                and sec_store.information["Labeled_ESG_Type"] != "Labeled Green"
                and sec_store.information["TCW_ESG"] == "TCW Green"
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 2
                sec_store.scores["Securitized_Score"] = 2
            elif (
                sec_store.information["ESG_Collateral_Type"]["ESG Collat Type"]
                in collat_type_2
                and sec_store.information["Labeled_ESG_Type"] != "Labeled Social"
                and sec_store.information["TCW_ESG"] == "TCW Social"
                and not "TBA " in sec_store.information["IssuerName"]
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 2
                sec_store.scores["Securitized_Score"] = 2
            elif (
                (pd.isna(sec_store.information["Labeled_ESG_Type"]))
                and pd.isna(sec_store.information["TCW_ESG"])
                and not "TBA " in sec_store.information["IssuerName"]
            ):
                sec_store.scores["Securitized_Score_unadjusted"] = 4
                sec_store.scores["Securitized_Score"] = 4

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level:
            - if securitized score between 1 and 2: Leading
            - if securitized score between 2 and 4: Average
            - if securitized score above 4: Poor
            - if securitized score 0: not scored
        """
        for s in self.securities:
            score = self.securities[s].scores["Securitized_Score"]
            self.securities[s].set_risk_overall_score(score)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order:
        1) Securitized Score is 5
        2) Is Labeled Bond
        3) Has ESG Collat Type
        4) Is TBA
        5) Is Leading
        6) Is not scored
        """
        for s in self.securities:
            score = self.securities[s].scores["Securitized_Score"]
            self.securities[s].level_5()

            if score == 5:
                self.securities[s].is_score_5("Securitized")

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
            elif (
                self.securities[s].information["ESG_Collateral_Type"]["G/S/S"] == "CLO"
            ):
                self.securities[s].is_CLO()
            elif (
                not self.securities[s].information["ESG_Collateral_Type"][
                    "ESG Collat Type"
                ]
                == "Unknown"
            ):
                self.securities[s].has_collat_type()

            elif " TBA " in self.securities[s].information["Security_Name"]:
                self.securities[s].is_TBA()

            elif score <= 2 and score > 0:
                self.securities[s].is_leading()
            elif score == 0:
                self.securities[s].is_not_scored()

    def iter(
        self, regions_df: pd.DataFrame, regions: dict, gics_d: dict, bclass_d: dict
    ) -> None:
        """
        - attach GICS information
        - attach exclusions
        - attach region
        - attach industry

        Parameters
        ----------
        regions_df: pd.DataFrame
            DataFrame of regions information
        regions: dict
            dictionary of all region objects
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass sub industries with bclass as key, bclass object as value
        """
        # attach GICS
        self.attach_gics(gics_d)

        # attach exclusions
        self.iter_exclusion()

        # attach region
        self.attach_region(regions_df, regions)

        # attach industry and sub industry
        self.attach_industry(gics_d, bclass_d)
