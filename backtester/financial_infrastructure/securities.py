import quantkit.core.financial_infrastructure.securities as securities
import pandas as pd


class SecurityStore(securities.SecurityStore):
    """
    Security Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors
        - security information

    Parameters
    ----------
    isin: str
        Security's isin
    information: dict
        dictionary of security specific information
    """

    def __init__(self, isin: str, information: dict, **kwargs) -> None:
        super().__init__(isin, information, **kwargs)
        self.allocation_df = pd.DataFrame()

    def iter(
        self,
        dict_fundamental: dict,
        dict_prices: dict,
        **kwargs,
    ) -> None:
        """
        - Attach Prices information
        - Attach Fundamental information

        Parameters
        ----------
        dict_fundamental: dict
            dictionary of tickers with fundamental information
        dict_prices: dict
            dictionary of tickers with price information
        """
        super().iter(
            dict_fundamental=dict_fundamental, dict_prices=dict_prices, **kwargs
        )
        self.attach_price_information(dict_prices)
        self.attach_fundamental_information(dict_fundamental)
