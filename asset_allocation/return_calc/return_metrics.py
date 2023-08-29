import numpy as np
import pandas as pd
import copy


class ReturnMetrics(object):
    """
    Base class for building return metrics

    Parameters
    ----------
    factors: list
        factors to run return calculation on
    """

    def __init__(self, universe):
        self.universe = universe
        self.universe_size = len(universe)

    @property
    def return_metrics_optimizer(self):
        """
        Forecaseted returns from return engine

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        raise NotImplementedError

    @property
    def return_metrics_intuitive(self):
        """
        return metrics for plotting needs to be human interpretable

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        raise NotImplementedError

    def get_max_return(self):
        """
        Get 0 basis max return

        Return
        ------
        float
            max return in return array
        """
        return np.max(self.return_metrics_optimizer)

    def get_min_return(self):
        """
        Get 0 basis min return but setting lower bound to be zero

        Return
        ------
        float
            min return in return array
        """
        return max(np.min(self.return_metrics_optimizer), 0.0)

    def assign(
        self,
        date,
        price_return,
        annualize_factor=None,
        **kwargs,
    ):
        """
        Transform and assign returns to the actual calculator
        Parameter
        ---------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency

        Return
        ------
        """
        raise NotImplementedError

    def get_portfolio_return(
        self,
        allocation,
        this_returns,
        indexes,
        next_allocation=None,
        trans_cost=0.0,
        **kwargs,
    ):
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameter
        ---------
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

        Return
        ------
        pd.DataFrame
            return: float

        """
        n_obs = len(this_returns)
        cumulative_returns = np.cumprod(this_returns + 1, axis=0)
        ending_allocation = allocation * cumulative_returns
        # Normalize ending allocation
        ending_allocation = (ending_allocation.T / np.sum(ending_allocation, axis=1)).T

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
