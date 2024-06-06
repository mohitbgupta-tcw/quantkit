import pandas as pd
import quantkit.core.financial_infrastructure.asset_base as asset_base


class SecuritizedStore(asset_base.AssetBase):
    """
    Securitized object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        securitized's isin
    """

    def __init__(self, isin: str, row_data: pd.Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)
        self.type = "securitized"

    def iter(
        self,
        **kwargs,
    ) -> None:
        """
        Iterate over securitized object and attach information
        """
        pass
