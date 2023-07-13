import numpy as np


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

    def __init__(self, isin: str, information: dict, **kwargs):
        self.isin = isin
        self.information = information
        self.portfolio_store = dict()
        self.scores = dict()

        self.information["ESG_Collateral_Type"] = dict()
        self.information["Labeled_ESG_Type"] = np.nan
        self.information["TCW_ESG"] = np.nan
        self.information["Issuer_ESG"] = "No"
        self.information["Security_Name"] = ""
        self.scores["Securitized_Score"] = 0
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


class EquityStore(SecurityStore):
    """
    Equity Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors
        - security information

    Parameters
    ----------
    isin: str
        Equity's isin
    information: dict
        dictionary of security specific information

    """

    def __init__(self, isin: str, information: dict, **kwargs):
        super().__init__(isin, information)


class FixedIncomeStore(SecurityStore):
    """
    Fixed Income Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors
        - security information

    Parameters
    ----------
    isin: str
        Fixed Income's isin
    information: dict
        dictionary of security specific information

    """

    def __init__(self, isin: str, information: dict, **kwargs):
        super().__init__(isin, information)
