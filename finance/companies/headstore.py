import pandas as pd
import numpy as np
import quantkit.finance.securities.securities as securities
import quantkit.finance.sectors.sectors as sectors
import quantkit.finance.adjustment.adjustment as adjustment
from typing import Union


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
        company's isin. NoISIN if no isin is available
    """

    def __init__(self, isin: str, **kwargs):
        self.isin = isin
        self.securities = dict()
        self.scores = dict()
        self.information = dict()

        # assign some default values for measures
        self.scores["Themes"] = dict()
        self.scores["Transition_Category"] = list()
        self.scores["Sustainability_Tag"] = "N"
        self.scores["Transition_Tag"] = "N"
        self.scores["Muni_Score"] = 0
        self.scores["Sovereign_Score"] = 0
        self.scores["ESRM_Score"] = 0
        self.scores["Governance_Score"] = 0
        self.scores["Target_Score"] = 0
        self.scores["Transition_Score"] = 0
        self.scores["Corporate_Score"] = 0
        self.scores["Review_Flag"] = ""
        self.scores["Review_Comments"] = ""
        self.scores["ESRM_Flags"] = dict()
        self.scores["Governance_Flags"] = dict()
        self.scores["NA_Flags_ESRM"] = dict()
        self.scores["NA_Flags_Governance"] = dict()
        self.Adjustment = pd.DataFrame(
            columns=["Thematic Type", "Category", "Adjustment"]
        )
        self.Exclusion = pd.DataFrame()
        self.information["Sector_Level_2"] = np.nan
        self.information["IVA_COMPANY_RATING"] = np.nan
        self.information["Exclusion"] = []

    def add_security(
        self,
        isin: str,
        store: Union[
            securities.EquityStore,
            securities.FixedIncomeStore,
            securities.SecurityStore,
        ],
    ) -> None:
        """
        Add security object to parent.
        Security could be stock or issued Fixed Income of company.

        Parameters
        ----------
        isin: str
            security's isin
        store: securities.EquityStore | securities.FixedIncomeStore | securities.SecurityStore:
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

    def attach_region(self, regions_df: pd.DataFrame, regions: dict) -> None:
        """
        Attach region information (including ISO2, name, sovereign score) to parent object
        Save region object in self.information["Issuer_Country"]

        Parameters
        ----------
        regions_df: pd.DataFrame
            DataFrame of regions information
        regions: dict
            dictionary of all region objects
        """
        # dict to map name to ISO2
        temp_regions = pd.Series(
            regions_df["ISO2"].values,
            index=regions_df["Country"],
        ).to_dict()

        # if issuer country is country name, map to ISO2
        # attach region object to company
        country = self.msci_information["ISSUER_CNTRY_DOMICILE"]
        country = temp_regions.get(country, country)
        self.information["Issuer_Country"] = regions[country]
        regions[country].add_company(self.isin, self)

    def attach_analyst_adjustment(self, adjustment_df: pd.DataFrame) -> None:
        """
        Attach analyst adjustment to company object
        Link to adjustment datasource by MSCI issuer id

        Parameters
        ----------
        adjustment_df: pd.Dataframe
            DataFrame of Analyst Adjustments
        """
        # attach analyst adjustment
        msci_issuerid = self.msci_information["ISSUERID"]
        adj_df = adjustment_df[adjustment_df["ISIN"] == msci_issuerid]
        if not adj_df.empty:
            self.Adjustment = pd.concat(
                [self.Adjustment, adj_df],
                ignore_index=True,
                sort=False,
            )

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
        for index, row in self.Adjustment.iterrows():
            thematic_type = row["Thematic Type"]
            cat = row["Category"]
            a = row["Adjustment"]
            comment = row["Comments"]
            func_ = getattr(adjustment, thematic_type)
            func_(
                store=self,
                adjustment=a,
                themes=themes,
                theme=cat,
                comment=comment,
            )

    def iter_exclusion(self) -> None:
        """
        Do exclusion for each parent.
        Attach if parent would be A8 or A9 excluded
        """
        # check for exclusions
        for index, row in self.Exclusion.iterrows():
            article = row["Article"]
            self.information["Exclusion"].append(article)

    def attach_gics(self, gics_d: dict, gics_sub: str = "Unassigned GICS") -> None:
        """
        Attach GICS object to parent store
        Save GICS object in self.information["GICS_SUB_IND"]

        Parameters
        ----------
        gics_d: dict
            dictionary of gics sub industries with gics as key, gics object as value
        gics_sub: str, optional
            GICS sub industry
        """
        # if we can't find GICS in store, create new one as 'Unassigned GICS'
        gics_d[gics_sub] = gics_d.get(
            gics_sub,
            sectors.GICS(gics_sub, pd.Series(gics_d["Unassigned GICS"].information)),
        )
        self.information["GICS_SUB_IND"] = gics_d[gics_sub]

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

    def attach_category(self, category_d) -> None:
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

    def attach_exclusion(self, exclusion_df: pd.DataFrame) -> None:
        """
        Attach exclusions from MSCI to company object
        Link to exclusion datasource by MSCI issuer id

        Parameters
        ----------
        exclusion_df: pd.DataFrame
            DataFrame of exclusions based on articles 8 and 9
        """
        # map exclusion based on Article 8 and 9
        msci_issuerid = self.msci_information["ISSUERID"]
        excl_df = exclusion_df[exclusion_df["MSCI Issuer ID"] == msci_issuerid]
        if not excl_df.empty:
            self.Exclusion = pd.concat(
                [self.Exclusion, excl_df],
                ignore_index=True,
                sort=False,
            )
