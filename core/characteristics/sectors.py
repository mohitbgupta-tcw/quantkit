import pandas as pd
import numpy as np
from typing import Union


class Industry(object):
    """
    Industry object.
    Logic on company level: Take GICS_Sub_Ind. If GICS is 'Unassigned GICS', take BCLASS_Level4
    Stores information such as:
        - industry name
        - transition risk and initial transition score based on risk (5 for high risk, 3 for low)
        - sub sectors (as store)
        - companies in that industry (as store)

    Parameters
    ----------
    name: str
        Industry name
    transition_risk: str
        transition risk decided by analyst, can either be 'High" or 'Low'
    """

    def __init__(
        self,
        name: str,
        transition_risk: str,
        **kwargs,
    ) -> None:
        self.name = name
        self.transition_risk = transition_risk
        self.sub_sectors = dict()
        self.companies = dict()
        if self.transition_risk == "High":
            self.initial_score = 5
        else:
            self.initial_score = 3

    def add_sub_sector(self, sub_sector) -> None:
        """
        Add sub sector object

        Parameters
        ----------
        sub_sector: BClass | GICS
        """
        ss_name = sub_sector.class_name
        self.sub_sectors[ss_name] = sub_sector


class BClass(object):
    """
    BCLASS_Level4 object.
    Stores information such as
        - class name
        - Analyst
        - ESRM Module
        - Transition Risk & Targets
        - sector (as store)
        - industry of BClass (as store)

    Parameters
    ----------
    class_name: str
        name of BClass_Level4
    row_information: pd.Series
        information about sub sector
    """

    def __init__(self, class_name: str, row_information: pd.Series) -> None:
        self.class_name = class_name
        self.information = row_information.to_dict()
        self.companies = dict()

    def add_sector(self, sector) -> None:
        """
        Add main sector object (GICS or BClass)

        Parameters
        ----------
        sector: Sector
            main sector
        """
        self.sector = sector

    def add_industry(self, industry: Industry) -> None:
        """
        Add main Industry object

        Parameters
        ----------
        industry: Industry
            industry object
        """
        self.industry = industry


class GICS(object):
    """
    GICS_Sub_Ind object.
    Stores information such as
        - class name
        - Analyst
        - ESRM Module
        - Transition Risk & Targets
        - sector (as store)
        - industry of BClass (as store)

    Parameters
    ----------
    class_name: str
        name of GICS_Sub_Ind
    row_information: pd.Series
        information about sub sector
    """

    def __init__(self, class_name: str, row_information: pd.Series) -> None:
        self.class_name = class_name
        self.information = row_information.to_dict()
        self.companies = dict()

    def add_sector(self, sector) -> None:
        """
        Add main sector object (GICS or BClass)

        Parameters
        ----------
        sector: Sector
            main sector
        """
        self.sector = sector

    def add_industry(self, industry: Industry) -> None:
        """
        Add main Industry object

        Parameters
        ----------
        industry: Industry
            industry object
        """
        self.industry = industry


class Sector(object):
    """
    Sector object.
    Sectors in our sense are GICS (for Equity) and BCLASS (for Fixed Income).
    Stores information such as:
        - name
        - sub sectors (as store, either BClass or GICS object)

    Parameters
    ----------
    name: str
        Sector name (either GICS or BCLASS)
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.sub_sectors = dict()

    def add_sub_sector(self, sub_sector: Union[BClass, GICS]) -> None:
        """
        Add sub sector object to sector

        Parameters
        ----------
        sub_sector: BClass | GICS
            BClass or GICS sub sector object
        """
        self.sub_sectors[sub_sector.class_name] = sub_sector
