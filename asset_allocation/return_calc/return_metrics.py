import numpy as np
import pandas as pd
import copy
import datetime


class ReturnMetrics(object):
    """
    Base class for building return metrics

    Parameters
    ----------
    universe: list
        investment universe
    """

    def __init__(self, universe: list) -> None:
        self.universe = universe
        self.universe_size = len(universe)

    @property
    def return_metrics_optimizer(self) -> np.array:
        """
        Forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        raise NotImplementedError

    @property
    def return_metrics_intuitive(self) -> np.array:
        """
        return metrics for plotting needs to be human interpretable

        Returns
        -------
        np.array
            returns
        """
        raise NotImplementedError

    def assign(
        self,
        date: datetime.date,
        price_return: np.array,
        annualize_factor: int = None,
        **kwargs,
    ) -> None:
        """
        Transform and assign returns to the actual calculator

        Parameters
        ---------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency
        """
        raise NotImplementedError

    def get_portfolio_return(
        self,
        allocation: np.array,
        this_returns: np.array,
        indexes: np.array,
        next_allocation: np.array = None,
        trans_cost: float = 0.0,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameters
        ----------
        allocation: np.array
            current allocation
        this_returns: np.array
            forecasted returns per asset
        indexes: np.array
            index column for returned DataFrame
            should be set to date range
        next_allocation: np.array, optional
            next allocation, used to calculate turnover and transaction costs
        trans_cost: float, optional
            transaction cost in %

        Returns
        -------
        pd.DataFrame
            return: float
        """
        n_obs = len(this_returns)
        cumulative_returns = np.cumprod(this_returns + 1, axis=0)
        cumulative_returns = np.where(
            np.isnan(cumulative_returns), 0, cumulative_returns
        )
        ending_allocation = allocation * cumulative_returns
        # Normalize ending allocation
        ending_allocation = (
            ending_allocation.T / np.nansum(ending_allocation, axis=1)
        ).T

        actual_returns = allocation @ cumulative_returns.T
        actual_returns = np.insert(actual_returns, 0, 1)
        actual_returns = np.diff(actual_returns) / actual_returns[:-1]

        # Subtract transaction costs
        next_allocation_m = copy.deepcopy(ending_allocation)
        next_allocation_m[-1] = next_allocation
        trans_cost_m = np.zeros((n_obs, self.universe_size))
        trans_cost_m[-1] = trans_cost
        if next_allocation is not None:
            turnover = abs(next_allocation_m - ending_allocation)
            this_trans_cost = (turnover * trans_cost_m).sum(axis=1)
            actual_returns -= this_trans_cost

        return pd.DataFrame(data=actual_returns, index=indexes, columns=["return"])
