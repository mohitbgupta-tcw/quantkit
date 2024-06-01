from copy import deepcopy
import pandas as pd
import numpy as np
import quantkit.core.characteristics.sectors as sectors
import quantkit.risk_framework.core.exclusions as exclusions
import quantkit.risk_framework.core.adjustment as adjustment
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
        self.information["transition_info"] = dict()
        self.scores = dict(
            Themes=dict(),
            Themes_unadjusted=dict(),
            Transition_Category=list(),
            Transition_Category_unadjusted=list(),
            Sustainability_Tag="N",
            Transition_Tag="N",
            Muni_Score=0,
            Muni_Score_unadjusted=0,
            Sovereign_Score=0,
            Sovereign_Score_unadjusted=0,
            ESRM_Score=0,
            ESRM_Score_unadjusted=0,
            Governance_Score=0,
            Governance_Score_unadjusted=0,
            Target_Score=0,
            Transition_Score=0,
            Transition_Score_unadjusted=0,
            Corporate_Score=0,
            Review_Flag="",
            Review_Comments="",
            ESRM_Flags=dict(),
            Governance_Flags=dict(),
            NA_Flags_ESRM=dict(),
            NA_Flags_Governance=dict(),
        )

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
            self.information["Transition_Risk_Module"] = gics_object.information[
                "Transition Risk Module"
            ]
            # attach company to industry
            gics_object.industry.issuers[self.key] = self
        else:
            bclass_object = bclass_d[bclass4]
            self.information["Industry"] = bclass_object.industry
            self.information["Sub-Industry"] = bclass_object
            self.information["Transition_Risk_Module"] = bclass_object.information[
                "Transition Risk Module"
            ]
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
        Attach exclusions from MSCI to company object
        Link to exclusion datasource by MSCI issuer id
        Attach if parent would be A8 or A9 excluded

        Parameters
        ----------
        exclusion_dict: dict
            dict of exclusions based on articles 8 and 9
        """
        # map exclusion based on Article 8 and 9
        msci_issuerid = self.msci_information["ISSUERID"]
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

    def attach_region(self, regions: dict) -> None:
        """
        Attach region information (including ISO2, name, sovereign score) to parent object
        Save region object in self.information["Issuer_Country"]

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
        """
        if not regions:
            return
        country = self.msci_information["ISSUER_CNTRY_DOMICILE"]
        country = np.nan if pd.isna(country) else country
        self.information["Issuer_Country"] = regions[country]
        regions[country].add_company(self.isin, self)

    def attach_category(self, category_d: dict) -> None:
        """
        Attach ESRM category based on ESRM module of sub industry
        Attach Region Theme based on Region

        Parameters
        ----------
        category_d: dict
            dictionary of ESRM categories
        """
        if not category_d:
            return
        esrm_module = self.information["Sub-Industry"].information["ESRM Module"]
        self.information["ESRM Module"] = category_d[esrm_module]

        # get region theme (DM, JP, EM, EUCohort)
        region_theme = self.information["Issuer_Country"].information["Region"]
        self.information["region_theme"] = category_d[region_theme]

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
