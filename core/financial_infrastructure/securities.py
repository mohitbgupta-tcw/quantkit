import numpy as np
import pandas as pd
from copy import deepcopy
import quantkit.core.characteristics.sectors as sectors


class SecurityStore(object):
    """
    Security Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - security information

    Parameters
    ----------
    isin: str
        Security's isin
    information: dict
        dictionary of security specific information
    """

    def __init__(self, isin: str, information: dict, **kwargs) -> None:
        self.isin = isin
        self.information = information
        self.portfolio_store = dict()

    def add_parent(self, parent) -> None:
        """
        Add parent store to security

        Parameters
        ----------
        parent: Company | Muni | Sovereign | Securitized
            parent store
        """
        self.parent_store = parent

    def overwrite_parent(self, parent_issuer_dict: dict, companies: dict) -> None:
        """
        Manually overwrite the parent of a security

        Parameters
        ----------
        parent_issuer_dict: dict
            dictionary of security to parent mapping
        companies: dict
            dictionary of all company objects
        """
        if self.isin in parent_issuer_dict:
            adj = parent_issuer_dict[self.isin]
            parent = adj["ISIN"]
            if parent in companies:
                prev_parent = self.parent_store.isin
                self.parent_store.remove_security(isin=self.isin)
                companies[parent].add_security(isin=self.isin, store=self)
                self.add_parent(companies[parent])
                if not companies[prev_parent].securities:
                    del companies[prev_parent]

    def attach_bclass(self, bclass_dict: dict) -> None:
        """
        Attach BCLASS object to security parent

        Parameters
        ----------
        bclass_dict: dict
            dictionary of all bclass objects
        """
        bclass4 = self.information["BCLASS_Level4"]
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

        # for first initialization of BCLASS
        self.parent_store.information[
            "BCLASS_Level4"
        ] = self.parent_store.information.get("BCLASS_Level4", bclass_object)
        # sometimes same security is labeled with different BCLASS_Level4
        # --> if it was unassigned before: overwrite, else: skipp
        if not (bclass_object.class_name == "Unassigned BCLASS"):
            self.parent_store.information["BCLASS_Level4"] = bclass_object
            bclass_object.companies[self.parent_store.isin] = self.parent_store

    def attach_rud_information(
        self,
        rud_dict: dict,
    ) -> None:
        """
        Attach bloomberg information to security parent

        Parameters
        ----------
        rud_dict: dict
            dictionary of research and development information
        """
        if self.information["BBG ISSUERID"] in rud_dict:
            rud_information = deepcopy(rud_dict[self.information["BBG ISSUERID"]])
            self.parent_store.rud_information = rud_information
        else:
            rud_information = deepcopy(rud_dict[np.nan])
            if not hasattr(self.parent_store, "rud_information"):
                self.parent_store.rud_information = rud_information

    def attach_iss_information(
        self,
        sdg_dict: dict,
    ) -> None:
        """
        Attach iss information to security parent

        Parameters
        ----------
        sdg_dict: dict
            dictionary of iss information
        """
        if self.information["ISS ISSUERID"] in sdg_dict:
            iss_information = deepcopy(sdg_dict[self.information["ISS ISSUERID"]])
            self.parent_store.sdg_information = iss_information
        else:
            iss_information = deepcopy(sdg_dict[np.nan])
            if not hasattr(self.parent_store, "sdg_information"):
                self.parent_store.sdg_information = iss_information

    def attach_price_information(
        self,
        dict_prices: dict,
    ) -> None:
        """
        Attach price information to security parent

        Parameters
        ----------
        dict_prices: dict
            dictionary of tickers with price information
        """
        if self.information["Ticker Cd"] in dict_prices:
            price_information = deepcopy(dict_prices[self.information["Ticker Cd"]])
            self.parent_store.prices_information = price_information
        else:
            price_information = deepcopy(dict_prices[np.nan])
            if not hasattr(self.parent_store, "prices_information"):
                self.parent_store.prices_information = price_information

    def attach_fundamental_information(
        self,
        dict_fundamental: dict,
    ) -> None:
        """
        Attach fundamental information to security parent

        Parameters
        ----------
        dict_fundamental: dict
            dictionary of tickers with fundamental information
        """
        if self.information["Ticker Cd"] in dict_fundamental:
            fundamental_information = deepcopy(
                dict_fundamental[self.information["Ticker Cd"]]
            )
            self.parent_store.fundamental_information = fundamental_information
        else:
            fundamental_information = deepcopy(dict_fundamental[np.nan])
            if not hasattr(self.parent_store, "fundamental_information"):
                self.parent_store.fundamental_information = fundamental_information

    def iter(self, **kwargs) -> None:
        """
        Iter over security and attach information
        """
        pass
