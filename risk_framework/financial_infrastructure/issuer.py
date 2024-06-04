from copy import deepcopy
import pandas as pd
import numpy as np
import quantkit.core.characteristics.sectors as sectors
import quantkit.risk_framework.core.exclusions as exclusions
from quantkit.core.financial_infrastructure.issuer import IssuerStore


class IssuerStore(IssuerStore):
    """
    Issuer object. Basket for Securities
    Stores information such as:
        - id
        - attached securities (Equity and Bonds)
        - industry (including GICS and BCLASS objects)
        - scores

    Parameters
    ----------
    key: str
        issuer's key
    information: dict
        dictionary of issuer specific information
    """

    def __init__(self, key: str, information: dict, **kwargs) -> None:
        super().__init__(key, information, **kwargs)
        self.transition_information = dict()
        self.esg_information = dict()
        self.Adjustment = list()

    def attach_bclass(self, bclass_dict: dict) -> None:
        """
        Attach BCLASS object issuer

        Parameters
        ----------
        bclass_dict: dict
            dictionary of all bclass objects
        """
        bclass4 = self.information["BCLASS_LEVEL4"]
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
        self.information["BCLASS_LEVEL4"] = bclass_object
        bclass_object.issuers[self.key] = self

    def attach_gics(self, gics_d: dict) -> None:
        """
        Attach GICS object to issuer

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        # if we can't find GICS in store, create new one as 'Unassigned GICS'
        gics_sub = self.information["GICS_LEVEL4"]

        if not gics_sub in gics_d:
            gics_d[gics_sub] = sectors.GICS(
                gics_sub, deepcopy(pd.Series(gics_d["Unassigned GICS"].information))
            )
            gics_d[gics_sub].add_industry(gics_d["Unassigned GICS"].industry)
            gics_d["Unassigned GICS"].industry.add_sub_sector(gics_d[gics_sub])

        self.information["GICS_LEVEL4"] = gics_d[gics_sub]
        gics_d[gics_sub].issuers[self.key] = self

    def attach_industry(self, gics_d: dict, bclass_d: dict) -> None:
        """
        Attach industry and sub industry object to issuer
        logic: take GICS information, if GICS is unassigned, take BCLASS
        Save industry object in self.information["Industry"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass_level4 with bclass as key, bclass object as value
        """
        gics_sub = self.information["GICS_LEVEL4"].information["GICS_SUB_IND"]
        bclass4 = self.information["BCLASS_LEVEL4"].information["BCLASS_Level4"]

        if gics_sub != "Unassigned GICS":
            gics_object = gics_d[gics_sub]
            self.information["Industry"] = gics_object.industry
            self.information["Sub-Industry"] = gics_object
            # attach company to industry
            gics_object.industry.issuers[self.key] = self
        else:
            bclass_object = bclass_d[bclass4]
            self.information["Industry"] = bclass_object.industry
            self.information["Sub-Industry"] = bclass_object
            # attach company to industry
            bclass_object.industry.issuers[self.key] = self

    def attach_rud_information(
        self,
        rud_information: dict,
        rud_information_parent: dict,
    ) -> None:
        """
        Attach bloomberg information to issuer

        Parameters
        ----------
        rud_information: dict
            dictionary of research and development information
        rud_information_parent: dict
            dictionary of research and development information of parent
        """
        self.rud_information = rud_information

        # assign RuD data for missing values
        if rud_information_parent:
            for item, val in self.rud_information.items():
                if pd.isna(val):
                    self.rud_information[item] = rud_information[item]

    def attach_iss_information(
        self,
        iss_information: dict,
        iss_information_parent: dict,
    ) -> None:
        """
        Attach iss information to issuer

        Parameters
        ----------
        iss_information: dict
            dictionary of iss information
        iss_information_parent: dict
            dictionary of iss information of parent
        """
        self.sdg_information = iss_information

        # assign iss data for missing values
        if iss_information_parent:
            for item, val in self.sdg_information.items():
                if pd.isna(val):
                    self.sdg_information[item] = iss_information_parent[item]

    def attach_msci_information(
        self,
        msci_information: dict,
        msci_information_parent: dict,
    ) -> None:
        """
        Attach msci information to issuer

        Parameters
        ----------
        msci_information: dict
            dictionary of msci information
        msci_information_parent: dict
            dictionary of msci information of parent
        """
        self.msci_information = msci_information

        # assign msci data for missing values
        if msci_information_parent:
            for item, val in self.msci_information.items():
                if pd.isna(val):
                    self.msci_information[item] = msci_information_parent[item]

    def attach_exclusion(self, exclusion_dict: dict) -> None:
        """
        Attach exclusions from MSCI to issuer object
        Link to exclusion datasource by MSCI issuer id
        Attach if parent would be A8 or A9 excluded

        Parameters
        ----------
        exclusion_dict: dict
            dict of exclusions based on articles 8 and 9
        """
        # map exclusion based on Article 8 and 9
        msci_issuerid = self.information["MSCI_ISSUERID"]
        self.exclusion_data = exclusion_dict.get(
            msci_issuerid, exclusion_dict["NoISSUERID"]
        )

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

        self.exclusion_data["A8"] = exclusions.A8(
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
        self.exclusion_data["A9"] = exclusions.A9(
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
        self.exclusion_data["Exclusion"] = list()
        if sum(self.exclusion_data["A8"].exclusion_dict.values()) > 0:
            self.exclusion_data["Exclusion"].append("Article 8")
        if sum(self.exclusion_data["A9"].exclusion_dict.values()) > 0:
            self.exclusion_data["Exclusion"].append("Article 9")

    def attach_analyst_adjustment(self, adjustment_dict: dict) -> None:
        """
        Attach analyst adjustment to issuer object
        Link to adjustment datasource by MSCI issuer id or security ISIN

        Parameters
        ----------
        adjustment_dict: dict
            dictionary of Analyst Adjustments
        """
        # attach analyst adjustment
        msci_issuerid = self.information["MSCI_ISSUERID"]
        if msci_issuerid in adjustment_dict:
            self.Adjustment = adjustment_dict[msci_issuerid]

        for sec, sec_store in self.securities.items():
            sec_isin = sec_store.information["ISIN"]
            if sec_isin in adjustment_dict:
                self.Adjustment = adjustment_dict[sec_isin]

    def calculate_capex(self) -> None:
        """
        Calculate the green CapEx of an issuer
        save capex in information[CapEx]

        Calculation
        -----------
            max(
                GreenExpTotalCapExSharePercent,
                RENEW_ENERGY_CAPEX_VS_TOTAL_CAPEX_PCT
            )
        """
        self.esg_information["CapEx"] = np.nanmax(
            [
                self.sdg_information["GreenExpTotalCapExSharePercent"] * 100,
                self.msci_information["RENEW_ENERGY_CAPEX_VS_TOTAL_CAPEX_PCT"],
                0,
            ]
        )

    def calculate_climate_revenue(self) -> None:
        """
        Calculate the green climate revenue of an issuer
        save climate revenue in information[Climate_Revenue]

        Calculation
        -----------
            max(
                CT_CC_TOTAL_MAX_REV,
                SDGSolClimatePercentCombCont
            )
        """
        self.esg_information["Climate_Revenue"] = np.nanmax(
            [
                self.msci_information["CT_CC_TOTAL_MAX_REV"],
                self.sdg_information["SDGSolClimatePercentCombCont"] * 100,
                0,
            ]
        )

    def calculate_climate_decarb(self) -> None:
        """
        Calculate the decarbonization improvement of an issuer
        """
        if self.msci_information["SALES_USD_RECENT"] > 0:
            carbon_rn = (
                self.msci_information["CARBON_EMISSIONS_SCOPE_12_INTEN"]
                + self.msci_information["CARBON_EMISSIONS_SCOPE_3"]
                / self.msci_information["SALES_USD_RECENT"]
            )
        else:
            carbon_rn = 0

        if self.msci_information["SALES_USD_FY19"] > 0:
            carbon_2019 = (
                self.msci_information["CARBON_EMISSIONS_SCOPE_12_INTEN_FY19"]
                + self.msci_information["CARBON_EMISSIONS_SCOPE_3_FY19"]
                / self.msci_information["SALES_USD_FY19"]
            )
        else:
            carbon_2019 = 0

        if carbon_2019 > 0:
            self.esg_information["Decarb"] = carbon_rn / carbon_2019 - 1
        else:
            self.esg_information["Decarb"] = 0

    def calculate_carbon_intensity(self) -> None:
        """
        Calculate the carbon intensity of an issuer

        Calculation
        -----------
            CARBON_EMISSIONS_SCOPE123 / SALES_USD_RECENT
        """
        if (
            self.msci_information["SALES_USD_RECENT"] > 0
            and self.msci_information["CARBON_EMISSIONS_SCOPE123"] > 0
        ):
            carbon_intensity = (
                self.msci_information["CARBON_EMISSIONS_SCOPE123"]
                / self.msci_information["SALES_USD_RECENT"]
            )
        # numerator or denominator are zero --> replace with median of sub industry
        else:
            carbon_intensity = self.information["Sub-Industry"].information[
                "Sub-Sector Median"
            ]

        self.esg_information["Carbon Intensity (Scope 123)"] = carbon_intensity

    def attach_transition_info(
        self,
        transition_company_mapping: dict,
    ) -> None:
        """
        Overwrite Enabler/Improver and Acronym tag for company specific companies

        Parameters
        ----------
        transition_company_mapping: dict
            companies with company specific improver/enabler tag
        """
        transition_info = deepcopy(self.information["Sub-Industry"].information)
        msci_id = self.information["MSCI_ISSUERID"]

        if msci_id in transition_company_mapping:
            transition_info["ENABLER_IMPROVER"] = transition_company_mapping[msci_id][
                "ENABLER_IMPROVER"
            ]
            transition_info["ACRONYM"] = transition_company_mapping[msci_id]["ACRONYM"]
        self.transition_information = transition_info
