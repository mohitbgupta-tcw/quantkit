import numpy as np


class SecurityStore(object):
    """
    Security Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors

    Parameters
    ----------
    isin: str
        Security's isin
    parent_store: companies.CompanyStore | companies.MuniStore | companies.SecuritizedStore | companies.SovereignStore
        parent the security belongs to

    """

    def __init__(self, isin: str, parent_store, **kwargs):
        self.isin = isin
        self.parent_store = parent_store
        self.portfolio_store = dict()
        self.information = dict()
        self.scores = dict()

        self.information["ESG_Collateral_Type"] = dict()
        self.information["Labeled_ESG_Type"] = np.nan
        self.information["TCW_ESG"] = np.nan
        self.information["Issuer_ESG"] = "No"
        self.information["Issuer_Name"] = ""
        self.scores["Securitized_Score"] = 0
        self.scores["Risk_Score_Overall"] = "Poor Risk Score"
        self.information["SClass_Level1"] = "Eligible"
        self.information["SClass_Level2"] = "ESG Scores"
        self.information["SClass_Level3"] = "ESG Scores"
        self.information["SClass_Level4"] = "Average ESG Score"
        self.information["SClass_Level4-P"] = "Average ESG Score"
        self.information["SClass_Level5"] = "Unknown"


class EquityStore(SecurityStore):
    """
    Equity Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors

    Parameters
    ----------
    isin: str
        Equity's isin
    parent_store: companies.CompanyStore | companies.MuniStore | companies.SecuritizedStore | companies.SovereignStore
        parent the security belongs to

    """

    def __init__(self, isin: str, parent_store, **kwargs):
        super().__init__(isin, parent_store)


class FixedIncomeStore(SecurityStore):
    """
    Fixed Income Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors

    Parameters
    ----------
    isin: str
        Fixed Income's isin
    parent_store: companies.CompanyStore | companies.MuniStore | companies.SecuritizedStore | companies.SovereignStore
        parent the security belongs to

    """

    def __init__(self, isin: str, parent_store, **kwargs):
        super().__init__(isin, parent_store)
