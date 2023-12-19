import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import numpy as np
import pandas as pd
from typing import Union
import datetime


class OriginalWeight(allocation_base.Allocation):
    """
    Base class to calculate Original Weight weighting scheme

    Parameters
    ----------
    asset_list: list
        all assets to run optimization on
    risk_engine: asset_allocation.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    return_engine: asset_allocation.return_calc.return_metrics
        return engine used to forecast returns
    """

    def __init__(self, asset_list: list, risk_engine, return_engine, **kwargs) -> None:
        super().__init__(asset_list, risk_engine, return_engine)

    def update(self, weights: np.ndarray, **kwargs) -> None:
        """
        Assign original weight

        Parameters
        ----------
        weights: np.array
            original weighting scheme for snapshot
        """
        self.original_weight = weights

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
        opt_allocation = np.divide(
            self.original_weight[selected_assets],
            np.nansum(self.original_weight[selected_assets]),
        )
        allocation[selected_assets] = opt_allocation

        self.allocations = (date, allocation)
        self.allocations_history[date] = allocation
