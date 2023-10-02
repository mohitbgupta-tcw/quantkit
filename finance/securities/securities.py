import numpy as np
import pandas as pd
from typing import Union
from copy import deepcopy
import quantkit.finance.sectors.sectors as sectors


class SecurityStore(object):
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
    isin: str
        Security's isin
    information: dict
        dictionary of security specific information

    """

    def __init__(self, isin: str, information: dict, **kwargs) -> None:
        self.isin = isin
        self.information = information
        self.portfolio_store = dict()
        self.scores = dict()

        self.scores["Securitized_Score"] = 0
        self.scores["Securitized_Score_unadjusted"] = 0
        self.scores["Risk_Score_Overall"] = "Poor Risk Score"
        self.information["SClass_Level1"] = "Eligible"
        self.information["SClass_Level2"] = "ESG Scores"
        self.information["SClass_Level3"] = "ESG Scores"
        self.information["SClass_Level4"] = "Average ESG Score"
        self.information["SClass_Level4-P"] = "Average ESG Score"
        self.information["SClass_Level5"] = "Unknown"

    def add_parent(self, parent) -> None:
        """
        Add parent store to security

        Parameters
        ----------
        parent: Company | Muni | Sovereign | Securitized
            parent store
        """
        self.parent_store = parent

    def add_collateral_type(self, securitized_mapping: dict) -> None:
        """
        Add ESG Collateral Type object to information

        Parameters
        ----------
        securitized_mapping: dict
            mapping for ESG Collat Type
        """
        if securitized_mapping:
            self.information["ESG Collateral Type"] = securitized_mapping[
                self.information["ESG Collateral Type"]
            ]

    def attach_bclass(self, bclass_dict: dict) -> None:
        """
        Attach BCLASS object to security parent

        Parameters
        ----------
        bclass_dict: dict
            dictionary of all bclass objects
        """
        if not bclass_dict:
            return
        bclass4 = self.information["BCLASS_Level4"]
        # attach BCLASS object
        # if BCLASS is not in BCLASS store (covered industries), attach 'Unassigned BCLASS'
        if not bclass4 in bclass_dict:
            bclass_dict[bclass4] = sectors.BClass(
                bclass4,
                deepcopy(pd.Series(bclass_dict["Unassigned BCLASS"].information)),
            )
            bclass_dict[bclass4].add_industry(bclass_dict["Unassigned BCLASS"].industry)
            bclass_dict["Unassigned BCLASS"].industry.add_sub_sector(
                bclass_dict[bclass4]
            )
        bclass_object = bclass_dict[bclass4]

        # for first initialization of BCLASS
        self.parent_store.information[
            "BCLASS_Level4"
        ] = self.parent_store.information.get("BCLASS_Level4", bclass_object)
        # sometimes same security is labeled with different BCLASS_Level4
        # --> if it was unassigned before: overwrite, else: skipp
        if not (bclass_object.class_name == "Unassigned BCLASS"):
            self.parent_store.information["BCLASS_Level4"] = bclass_object
            bclass_object.companies[self.parent_store.isin] = self.parent_store

    def attach_sector_level_2(self) -> None:
        """
        Attach Sector Level 2 to security parent
        """
        sector_level_2 = self.information["Sector Level 2"]
        self.parent_store.information["Sector_Level_2"] = sector_level_2

    def attach_bloomberg_information(
        self,
        bloomberg_dict: dict,
    ) -> None:
        """
        Attach bloomberg information to security parent

        Parameters
        ----------
        bloomberg_dict: dict
            dictionary of bloomberg information
        """
        if not bloomberg_dict:
            return
        if self.information["BBG ISSUERID"] in bloomberg_dict:
            bbg_information = deepcopy(bloomberg_dict[self.information["BBG ISSUERID"]])
            self.parent_store.bloomberg_information = bbg_information
        else:
            bbg_information = deepcopy(bloomberg_dict[np.nan])
            if not hasattr(self.parent_store, "bloomberg_information"):
                self.parent_store.bloomberg_information = bbg_information

    def attach_iss_information(
        self,
        sdg_dict: dict,
    ) -> None:
        """
        Attach iss information to security parent

        Parameters
        ----------
        sdg_dict: dict
            dictionary of iss information
        """
        if not sdg_dict:
            return
        if self.information["ISS ISSUERID"] in sdg_dict:
            iss_information = deepcopy(sdg_dict[self.information["ISS ISSUERID"]])
            self.parent_store.sdg_information = iss_information
        else:
            iss_information = deepcopy(sdg_dict[np.nan])
            if not hasattr(self.parent_store, "sdg_information"):
                self.parent_store.sdg_information = iss_information

    def attach_quandl_information(
        self,
        quandl_dict_fundamental: dict,
        quandl_dict_prices: dict,
    ) -> None:
        """
        Attach iss information to security parent

        Parameters
        ----------
        quandl_dict_fundamental: dict
            dictionary of quandl fundamental information
        quandl_dict_prices: dict
            dictionary of quandl price information
        """
        if quandl_dict_fundamental:
            if self.information["Ticker Cd"] in quandl_dict_fundamental:
                quandl_information = deepcopy(
                    quandl_dict_fundamental[self.information["Ticker Cd"]]
                )
                self.parent_store.quandl_information = quandl_information
            else:
                quandl_information = deepcopy(quandl_dict_fundamental[np.nan])
                if not hasattr(self.parent_store, "quandl_information"):
                    self.parent_store.quandl_information = quandl_information
        if quandl_dict_prices:
            if self.information["Ticker Cd"] in quandl_dict_prices:
                quandl_information = deepcopy(
                    quandl_dict_prices[self.information["Ticker Cd"]]
                )
                self.parent_store.quandl_information_prices = quandl_information
            else:
                quandl_information = deepcopy(quandl_dict_prices[np.nan])
                if not hasattr(self.parent_store, "quandl_information"):
                    self.parent_store.quandl_information_prices = quandl_information

    def attach_analyst_adjustment(
        self,
        sec_adjustment_dict: dict,
    ) -> None:
        """
        Attach Analyst Adjustment to parent
        Adjustment in sec_adjustment_dict is on security level

        Parameters
        ----------
        sec_adjustment_dict: dict
            dictionary with analyst adjustment information
        """
        if self.isin in sec_adjustment_dict:
            self.parent_store.Adjustment = sec_adjustment_dict[self.isin]

    def overwrite_parent(self, parent_issuer_dict: dict, companies: dict) -> None:
        """
        Manually overwrite the parent of a security

        Parameters
        ----------
        parent_issuer_dict: dict
            dictionary of security to parent mapping
        companies: dict
            dictionary of all company objects
        """
        if self.isin in parent_issuer_dict:
            adj = parent_issuer_dict[self.isin]
            parent = adj["ISIN"]
            if parent in companies:
                prev_parent = self.parent_store.isin
                self.parent_store.remove_security(isin=self.isin)
                companies[parent].add_security(isin=self.isin, store=self)
                self.add_parent(companies[parent])
                if not companies[prev_parent].securities:
                    del companies[prev_parent]

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
        sustainability_category = self.parent_store.scores["Themes"]
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
            self.information["SClass_Level4-P"] = self.parent_store.information[
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
        transition_category = self.parent_store.scores["Transition_Category"]
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

    def iter(
        self,
        parent_issuer_dict: dict,
        companies: dict,
        securitized_mapping: dict,
        bclass_dict: dict,
        sec_adjustment_dict: dict,
        bloomberg_dict: dict,
        sdg_dict: dict,
        quandl_dict_fundamental: dict,
        quandl_dict_prices: dict,
    ) -> None:
        """
        - Add ESG Collateral Type
        - Attach BClass Level4 to parent

        Parameters
        ----------
        parent_issuer_dict: dict
            dictionary of security to parent mapping
        companies: dict
            dictionary of all company objects
        securitized_mapping: dict
            mapping for ESG Collat Type
        bclass_dict: dict
            dictionary of all bclass objects
        sec_adjustment_dict: dict
            dictionary with analyst adjustment information
        bloomberg_dict: dict
            dictionary of bloomberg information
        sdg_dict: dict
            dictionary of iss information
        quandl_dict_fundamental: dict
            dictionary of quandl fundamental information
        quandl_dict_prices: dict
            dictionary of quandl price information
        """
        self.overwrite_parent(parent_issuer_dict, companies)
        self.add_collateral_type(securitized_mapping)
        self.attach_bclass(bclass_dict)
        self.attach_sector_level_2()
        self.attach_analyst_adjustment(sec_adjustment_dict)
        self.attach_bloomberg_information(bloomberg_dict)
        self.attach_iss_information(sdg_dict)
        self.attach_quandl_information(quandl_dict_fundamental, quandl_dict_prices)
