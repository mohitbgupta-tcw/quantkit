from pandas import Series
import pandas as pd
import quantkit.core.financial_infrastructure.securitized as securitized
import quantkit.risk_framework.financial_infrastructure.asset_base as asset_base


class SecuritizedStore(asset_base.AssetBase, securitized.SecuritizedStore):
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
        - attach GICS information
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

        # attach GICS
        self.attach_gics(gics_d)

        # attach region
        self.attach_region(regions)

        # attach industry and sub industry
        self.attach_industry(gics_d, bclass_d)

        # attach exclusion df
        self.attach_exclusion(exclusion_dict)
