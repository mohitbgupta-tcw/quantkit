import quantkit.mathstats.median.median as median
import pandas as pd
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
        - Q_Low, Q_Low_score (lowest quantile)
        - Q_High, Q_High_score (highest quantile)

    Parameters
    ----------
    name: str
        Industry name
    transition_risk: str
        transition risk decided by analyst, can either be 'High" or 'Low'
    Q_Low: float
        Lowest Quantile (used for transition score). Companies with carbon intensity below this value will be marked as good
    Q_High: float
        Highest Quantile (used for transition score). Companies with carbon intensity over this value will be marked as bad
    """

    def __init__(
        self, name: str, transition_risk: str, Q_Low: float, Q_High: float, **kwargs
    ):
        self.name = name
        self.transition_risk = transition_risk
        self.sub_sectors = dict()
        self.companies = dict()
        self.carbon_median = median.Quantiles()
        self.Q_Low = Q_Low
        self.Q_High = Q_High
        self.Q_Low_score = 0
        self.Q_High_score = 0
        if self.transition_risk == "High":
            self.initial_score = 5
        else:
            self.initial_score = 3

    def calculate_quantiles(self):
        """
        Calculate quantiles for carbon intensity based on Q_Low and Q_High

        Returns
        -------
        np.array
            array with two elements (lower and higher quantile)
        """
        return self.carbon_median.quantiles([self.Q_Low, self.Q_High])

    @property
    def median(self):
        """
        Median for carbon intensity out of all companies attached to this Industry

        Returns
        -------
        float
            carbon intensity median for Industry
        """
        return self.carbon_median.median

    def update(self, carbon_intensity: float):
        """
        update Industry information such as collecting carbon intensity whenever company is added to Industry
        and calcualte quantiles based on new value, update Industry specific Q_Low and Q_High

        Parameters
        ----------
        carbon_intensity: float
            carbon intensity for company
        """
        self.carbon_median.add_value(carbon_intensity)
        self.quantiles = self.calculate_quantiles()
        self.Q_Low_score, self.Q_High_score = self.quantiles[0], self.quantiles[1]


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

    def __init__(self, name: str):
        self.name = name
        self.sub_sectors = dict()

    def add_sub_sector(self, sub_sector):
        self.sub_sectors[sub_sector.class_name] = sub_sector
        return


class BClass(object):
    """
    BCLASS_Level4 object.
    Stores information such as
        - class name
        - Analyst
        - ESRM Module
        - Transition Risk
        - sector (as store)
        - industry of BClass (as store)

    Parameters
    ----------
    class_name: str
        name of BClass_Level4
    row_information: pd.Series
        information about sub sector
    """

    def __init__(self, class_name: str, row_information: pd.Series):
        self.class_name = class_name
        self.information = row_information.to_dict()

    def add_sector(self, sector):
        self.sector = sector
        return


class GICS(object):
    """
    GICS_Sub_Ind object.
    Stores information such as
        - class name
        - Analyst
        - ESRM Module
        - Transition Risk
        - sector (as store)
        - industry of BClass (as store)

    Parameters
    ----------
    class_name: str
        name of GICS_Sub_Ind
    row_information: pd.Series
        information about sub sector
    """

    def __init__(self, class_name: str, row_information: pd.Series):
        self.class_name = class_name
        self.information = row_information.to_dict()

    def add_sector(self, sector):
        self.sector = sector
        return
