import numpy as np
import pandas as pd
from copy import deepcopy
import quantkit.core.characteristics.sectors as sectors


class SecurityStore(object):
    """
    Security Object
    Stores information such as:
        - key
        - parent (as store)
        - portfolios the security is held in (as store)
        - security information

    Parameters
    ----------
    key: str
        Security's key
    information: dict
        dictionary of security specific information
    """

    def __init__(self, key: str, information: dict, **kwargs) -> None:
        self.key = key
        self.information = information
        self.portfolio_store = dict()

    def add_issuer(self, issuer) -> None:
        """
        Add parent store to security

        Parameters
        ----------
        parent: IssuerStore
            issuer store
        """
        self.issuer_store = issuer

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
            self.issuer_store.prices_information = price_information
        else:
            price_information = deepcopy(dict_prices[np.nan])
            if not hasattr(self.issuer_store, "prices_information"):
                self.issuer_store.prices_information = price_information

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
            self.issuer_store.fundamental_information = fundamental_information
        else:
            fundamental_information = deepcopy(dict_fundamental[np.nan])
            if not hasattr(self.issuer_store, "fundamental_information"):
                self.issuer_store.fundamental_information = fundamental_information
