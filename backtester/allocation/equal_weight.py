import quantkit.backtester.allocation.allocation_base as allocation_base
import numpy as np
import pandas as pd
from typing import Union
import datetime


class EqualWeight(allocation_base.Allocation):
    """
    Base class to calculate Equal Weight weighting scheme

    Calculation
    -----------
    asset weight = 1 / number of assets

    Parameters
    ----------
    asset_list: list
        all assets to run optimization on
    risk_engine: backtester.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    return_engine: backtester.return_calc.return_metrics
        return engine used to forecast returns
    portfolio_leverage: float, optional
        portfolio leverage
    """

    def __init__(
        self,
        asset_list: list,
        risk_engine,
        return_engine,
        portfolio_leverage: float = 1.0,
        **kwargs,
    ) -> None:
        super().__init__(
            asset_list,
            risk_engine,
            return_engine,
            portfolio_leverage=portfolio_leverage,
        )

    def update(self, **kwargs) -> None:
        """
        None
        """
        return

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
        asset_count = len(selected_assets)
        allocation = np.zeros(shape=self.num_total_assets)
        opt_allocation = tuple(
            np.array([1 / asset_count] * asset_count) * self.portfolio_leverage
        )
        allocation[selected_assets] = opt_allocation

        self.allocations = (date, allocation)
        self.allocations_history[date] = allocation
