import pandas as pd
import quantkit.core.financial_infrastructure.asset_base as asset_base
import quantkit.risk_framework.core.adjustment as adjustment
import quantkit.risk_framework.core.exclusions as exclusions


class AssetBase(asset_base.AssetBase):
    """
    HeadStore object. Basket for different assets
    Stores information such as:
        - isin
        - attached securities (Equity and Bonds)
        - industry (including GICS and BCLASS objects)
        - scores

    Parameters
    ----------
    isin: str
        company's isin
    row_data: pd.Series
        msci information
    """

    def __init__(self, isin: str, row_data: pd.Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)
        self.scores = dict()

        # assign some default values for measures
        self.scores["Themes"] = dict()
        self.scores["Themes_unadjusted"] = dict()
        self.scores["Transition_Category"] = list()
        self.scores["Transition_Category_unadjusted"] = list()
        self.scores["Sustainability_Tag"] = "N"
        self.scores["Transition_Tag"] = "N"
        self.scores["Muni_Score"] = 0
        self.scores["Muni_Score_unadjusted"] = 0
        self.scores["Sovereign_Score"] = 0
        self.scores["Sovereign_Score_unadjusted"] = 0
        self.scores["ESRM_Score"] = 0
        self.scores["ESRM_Score_unadjusted"] = 0
        self.scores["Governance_Score"] = 0
        self.scores["Governance_Score_unadjusted"] = 0
        self.scores["Target_Score"] = 0
        self.scores["Transition_Score"] = 0
        self.scores["Transition_Score_unadjusted"] = 0
        self.scores["Corporate_Score"] = 0
        self.scores["Review_Flag"] = ""
        self.scores["Review_Comments"] = ""
        self.scores["ESRM_Flags"] = dict()
        self.scores["Governance_Flags"] = dict()
        self.scores["NA_Flags_ESRM"] = dict()
        self.scores["NA_Flags_Governance"] = dict()
        self.Adjustment = list()
        self.Exclusion = dict()
        self.information["Exclusion"] = list()

    def attach_analyst_adjustment(self, msci_adjustment_dict: dict) -> None:
        """
        Attach analyst adjustment to company object
        Link to adjustment datasource by MSCI issuer id

        Parameters
        ----------
        msci_adjustment_dict: dict
            dictionary of Analyst Adjustments
        """
        # attach analyst adjustment
        msci_issuerid = self.msci_information["ISSUERID"]
        if msci_issuerid in msci_adjustment_dict:
            self.Adjustment = msci_adjustment_dict[msci_issuerid]

    def iter_analyst_adjustment(self, themes: dict) -> None:
        """
        Do analyst adjustments for each parent.
        Different calculations for each thematic type:
            - Risk
            - Transition
            - People
            - Planet
        See quantkit.finance.adjustments for more information

        Parameters
        ----------
        themes: dict
            dictionary of all themes
        """
        # check for analyst adjustment
        for adj in self.Adjustment:
            thematic_type = adj["Thematic Type"]
            cat = adj["Category"]
            a = adj["Adjustment"]
            comment = adj["Comments"]
            func_ = getattr(adjustment, thematic_type)
            func_(
                store=self,
                adjustment=a,
                themes=themes,
                theme=cat,
                comment=comment,
            )

    def attach_exclusion(self, exclusion_dict: dict) -> None:
        """
        Attach exclusions from MSCI to company object
        Link to exclusion datasource by MSCI issuer id
        Attach if parent would be A8 or A9 excluded

        Parameters
        ----------
        exclusion_dict: dict
            dict of exclusions based on articles 8 and 9
        """
        if not exclusion_dict:
            return
        # map exclusion based on Article 8 and 9
        msci_issuerid = self.msci_information["ISSUERID"]
        if msci_issuerid in exclusion_dict:
            self.exclusion_data = exclusion_dict[msci_issuerid]
        else:
            self.exclusion_data = exclusion_dict["NoISSUERID"]

        parent_cols = [
            "THERMAL_COAL_MAX_REV_PCT",
            "UNCONV_OIL_GAS_MAX_REV_PCT",
            "OG_REV",
            "GENERAT_MAX_REV_THERMAL_COAL",
            "UNGC_COMPLIANCE",
            "HR_COMPLIANCE",
            "IVA_COMPANY_RATING",
        ]
        for col in parent_cols:
            self.exclusion_data[col] = self.msci_information[col]

        self.Exclusion["A8"] = exclusions.A8(
            cweap_tie=self.exclusion_data["CWEAP_TIE"],
            weap_max_rev_pct=self.exclusion_data["WEAP_MAX_REV_PCT"],
            firearm_max_rev_pct=self.exclusion_data["FIREARM_MAX_REV_PCT"],
            tob_max_rev_pct=self.exclusion_data["TOB_MAX_REV_PCT"],
            thermal_coal_max_rev_pct=self.exclusion_data["THERMAL_COAL_MAX_REV_PCT"],
            unconv_oil_gas_max_rev_pct=self.exclusion_data[
                "UNCONV_OIL_GAS_MAX_REV_PCT"
            ],
            generat_max_rev_thermal_coal=self.exclusion_data[
                "GENERAT_MAX_REV_THERMAL_COAL"
            ],
            ungc_compliance=self.exclusion_data["UNGC_COMPLIANCE"],
            iva_company_rating=self.exclusion_data["IVA_COMPANY_RATING"],
        )
        self.Exclusion["A9"] = exclusions.A9(
            cweap_tie=self.exclusion_data["CWEAP_TIE"],
            weap_max_rev_pct=self.exclusion_data["WEAP_MAX_REV_PCT"],
            firearm_max_rev_pct=self.exclusion_data["FIREARM_MAX_REV_PCT"],
            tob_max_rev_pct=self.exclusion_data["TOB_MAX_REV_PCT"],
            thermal_coal_max_rev_pct=self.exclusion_data["THERMAL_COAL_MAX_REV_PCT"],
            unconv_oil_gas_max_rev_pct=self.exclusion_data[
                "UNCONV_OIL_GAS_MAX_REV_PCT"
            ],
            og_rev=self.exclusion_data["OG_REV"],
            generat_max_rev_thermal_coal=self.exclusion_data[
                "GENERAT_MAX_REV_THERMAL_COAL"
            ],
            ae_max_rev_pct=self.exclusion_data["AE_MAX_REV_PCT"],
            alc_dist_max_rev_pct=self.exclusion_data["ALC_DIST_MAX_REV_PCT"],
            alc_prod_max_rev_pct=self.exclusion_data["ALC_PROD_MAX_REV_PCT"],
            gam_max_rev_pct=self.exclusion_data["GAM_MAX_REV_PCT"],
            ungc_compliance=self.exclusion_data["UNGC_COMPLIANCE"],
            hr_compliance=self.exclusion_data["HR_COMPLIANCE"],
            iva_company_rating=self.exclusion_data["IVA_COMPANY_RATING"],
        )
        if sum(self.Exclusion["A8"].exclusion_dict.values()) > 0:
            self.information["Exclusion"].append("Article 8")
        if sum(self.Exclusion["A9"].exclusion_dict.values()) > 0:
            self.information["Exclusion"].append("Article 9")
