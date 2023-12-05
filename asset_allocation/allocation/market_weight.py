import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import numpy as np
from typing import Union
import datetime


class MarketWeight(allocation_base.Allocation):
    """
    Base class to calculate Market Weight weighting scheme

    Calculation
    -----------
    asset weight = market cap asset / sum(market cap)

    Parameters
    ----------
    asset_list: list
        all assets to run optimization on
    risk_engine: mstar_asset_allocation.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    return_engine: mstar_asset_allocation.return_calc.return_metrics
        return engine used to forecast returns
    """

    def __init__(self, asset_list: list, risk_engine, return_engine, **kwargs) -> None:
        super().__init__(asset_list, risk_engine, return_engine)

    def update(self, market_caps: np.ndarray, **kwargs) -> None:
        """
        - assign market caps

        Parameters
        ----------
        market_caps: np.array
            array of market caps of all companies in universe
        """
        market_caps = np.where(np.isnan(market_caps), 0, market_caps)
        self.market_caps = market_caps.squeeze()

    def allocate(
        self, date: datetime.date, selected_assets: Union[list, np.ndarray]
    ) -> None:
        """
        Solve for optimal portfolio and save allocation

        Parameters
        ----------
        date : datetime.date
            date of snapshot
        selected_assets: list | np.array
            list of selected assets (their location in universe as integer)
        """
        allocation = np.zeros(shape=self.num_total_assets)
        mc = self.market_caps[selected_assets]
        opt_allocation = mc / np.nansum(mc)
        allocation[selected_assets] = opt_allocation

        self.allocations = (date, allocation)
        self.allocations_history[date] = allocation
