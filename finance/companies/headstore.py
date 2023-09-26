import pandas as pd
import numpy as np
from copy import deepcopy
import quantkit.finance.securities.securities as securities
import quantkit.finance.sectors.sectors as sectors
import quantkit.finance.adjustment.adjustment as adjustment
import quantkit.utils.mapping_configs as mapping_configs


class HeadStore(object):
    """
    HeadStore object. Basket for Securities
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
        self.isin = isin
        self.securities = dict()
        self.scores = dict()
        self.msci_information = row_data
        self.information = dict()

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
        self.information["Exclusion_d"] = dict()
        self.information["Exclusion"] = list()

    def add_security(
        self,
        isin: str,
        store: securities.SecurityStore,
    ) -> None:
        """
        Add security object to parent.
        Security could be stock or issued Fixed Income of company.

        Parameters
        ----------
        isin: str
            security's isin
        store: SecurityStore
            security store of new security
        """
        self.securities[isin] = store

    def remove_security(self, isin: str) -> None:
        """
        Remove security object from company.

        Parameters
        ----------
        isin: str
            security's isin
        """
        self.securities.pop(isin, None)

    def attach_region(self, regions: dict) -> None:
        """
        Attach region information (including ISO2, name, sovereign score) to parent object
        Save region object in self.information["Issuer_Country"]

        Parameters
        ----------
        regions: dict
            dictionary of all region objects
        """
        country = self.msci_information["ISSUER_CNTRY_DOMICILE"]
        country = np.nan if pd.isna(country) else country
        self.information["Issuer_Country"] = regions[country]
        regions[country].add_company(self.isin, self)

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
        # map exclusion based on Article 8 and 9
        msci_issuerid = self.msci_information["ISSUERID"]
        if msci_issuerid in exclusion_dict:
            excl = exclusion_dict[msci_issuerid]

            for e in excl:
                article = e["Article"]
                exclusion = e["Field Name"]
                theme = mapping_configs.exclusions[exclusion]
                value = e["Field Value"]
                if theme in self.information["Exclusion_d"]:
                    self.information["Exclusion_d"][theme] = max(
                        self.information["Exclusion_d"][theme], value
                    )
                else:
                    self.information["Exclusion_d"][theme] = value
                self.information["Exclusion"].append(article)

    def attach_gics(self, gics_d: dict) -> None:
        """
        Attach GICS object to parent store
        Save GICS object in self.information["GICS_SUB_IND"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        # if we can't find GICS in store, create new one as 'Unassigned GICS'
        gics_sub = self.msci_information["GICS_SUB_IND"]
        if pd.isna(gics_sub):
            gics_sub = "Unassigned GICS"

        if not gics_sub in gics_d:
            gics_d[gics_sub] = sectors.GICS(
                gics_sub, deepcopy(pd.Series(gics_d["Unassigned GICS"].information))
            )
            gics_d[gics_sub].add_industry(gics_d["Unassigned GICS"].industry)
            gics_d["Unassigned GICS"].industry.add_sub_sector(gics_d[gics_sub])
        self.information["GICS_SUB_IND"] = gics_d[gics_sub]
        gics_d[gics_sub].companies[self.isin] = self

    def attach_industry(self, gics_d: dict, bclass_d: dict) -> None:
        """
        Attach industry and sub industry object to parent store
        logic: take GICS information, if GICS is unassigned, take BCLASS
        Save industry object in self.information["Industry"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        bclass_d: dict
            dictionary of bclass_level4 with bclass as key, bclass object as value
        """
        gics_sub = self.information["GICS_SUB_IND"].information["GICS_SUB_IND"]
        bclass4 = self.information["BCLASS_Level4"].information["BCLASS_Level4"]

        if gics_sub != "Unassigned GICS":
            gics_object = gics_d[gics_sub]
            self.information["Industry"] = gics_object.industry
            self.information["Sub-Industry"] = gics_object
            self.information["Transition_Risk_Module"] = gics_object.information[
                "Transition Risk Module"
            ]
            # attach company to industry
            gics_object.industry.companies[self.isin] = self
        else:
            bclass_object = bclass_d[bclass4]
            self.information["Industry"] = bclass_object.industry
            self.information["Sub-Industry"] = bclass_object
            self.information["Transition_Risk_Module"] = bclass_object.information[
                "Transition Risk Module"
            ]
            # attach company to industry
            bclass_object.industry.companies[self.isin] = self

    def attach_category(self, category_d: dict) -> None:
        """
        Attach ESRM category based on ESRM module of sub industry
        Attach Region Theme based on Region

        Parameters
        ----------
        category_d: dict
            dictionary of ESRM categories
        """

        esrm_module = self.information["Sub-Industry"].information["ESRM Module"]
        self.information["ESRM Module"] = category_d[esrm_module]

        # get region theme (DM, JP, EM, EUCohort)
        region_theme = self.information["Issuer_Country"].information["Region"]
        self.information["region_theme"] = category_d[region_theme]
