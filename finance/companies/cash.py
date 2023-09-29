import pandas as pd
import quantkit.finance.companies.headstore as headstore


class CashStore(headstore.HeadStore):
    """
    Cash object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        company's isin
    row_data: pd.Series
        msci information
    """

    def __init__(self, isin: str, row_data: pd.Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)
        self.type = "cash"

    def iter(
        self,
        regions: dict,
        gics_d: dict,
        bclass_d: dict,
    ) -> None:
        """
        - attach GICS information
        - attach region
        - attach industry

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
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

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level

        Rules
        -----
            - cash is not scored
        """
        for sec, sec_store in self.securities.items():
            sec_store.set_risk_overall_score(0)

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based

        Order
        -----
        1) Analyst Adjustment
        2) Is not Scored
        """
        transition_tag = self.scores["Transition_Tag"]
        sustainability_tag = self.scores["Sustainability_Tag"]
        for sec, sec_store in self.securities.items():
            sec_store.level_5()

            if sustainability_tag == "Y*":
                sec_store.is_sustainable()

            elif transition_tag == "Y*":
                sec_store.is_transition()
            else:
                sec_store.is_not_scored()
