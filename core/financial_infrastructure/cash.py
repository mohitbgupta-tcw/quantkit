import pandas as pd
import quantkit.core.financial_infrastructure.asset_base as asset_base


class CashStore(asset_base.AssetBase):
    """
    Cash object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)

    Parameters
    ----------
    isin: str
        company's isin
    row_data: pd.Series
        msci information
    """

    def __init__(self, isin: str, row_data: pd.Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)
        self.type = "cash"

    def iter(
        self,
        **kwargs,
    ) -> None:
        """
        Iterate over cash object and attach information
        """
        pass
