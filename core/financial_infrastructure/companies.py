import pandas as pd
import quantkit.core.financial_infrastructure.asset_base as asset_base


class CompanyStore(asset_base.AssetBase):
    """
    Company object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)
        - company derived from data providers

    Parameters
    ----------
    isin: str
        company's isin
    row_data: pd.Series
        company information derived from MSCI
    """

    def __init__(self, isin: str, row_data: pd.Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)
        self.type = "company"

    def iter(
        self,
        **kwargs,
    ) -> None:
        """
        Iterate over company object
        """
        pass
