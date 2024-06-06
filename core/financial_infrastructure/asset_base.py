import pandas as pd
import numpy as np
from copy import deepcopy
import quantkit.core.financial_infrastructure.securities as securities
import quantkit.core.characteristics.sectors as sectors


class AssetBase(object):
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
        self.msci_information = row_data
        self.information = dict()

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
        if not regions:
            return
        country = self.msci_information["ISSUER_CNTRY_DOMICILE"]
        country = np.nan if pd.isna(country) else country
        self.information["Issuer_Country"] = regions[country]
        regions[country].add_company(self.isin, self)

    def attach_gics(self, gics_d: dict) -> None:
        """
        Attach GICS object to parent store
        Save GICS object in self.information["GICS_SUB_IND"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        """
        if not gics_d:
            return
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
        if not gics_d and not bclass_d:
            return
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
        if not category_d:
            return
        esrm_module = self.information["Sub-Industry"].information["ESRM Module"]
        self.information["ESRM Module"] = category_d[esrm_module]

        # get region theme (DM, JP, EM, EUCohort)
        region_theme = self.information["Issuer_Country"].information["Region"]
        self.information["region_theme"] = category_d[region_theme]
