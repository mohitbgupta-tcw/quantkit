import quantkit.mathstats.optimizer.convex_optimizer as convex_optimizer
import numpy as np
from typing import Union


class PortfolioOptimizer(convex_optimizer.CVXPYOptimizer):
    """
    Base class for Portfolio Optimization

    Parameters
    ----------
    universe: list
        investment universe
    long_only: bool, optional
        allow long only portfolio or add short positions
    leverage: float, optional
        portfolio leverage, if leverage is None, solve for optimal leverage
    verbose: bool, optional
        verbose flag for solver
    """

    def __init__(
        self,
        universe: list,
        long_only: bool = True,
        leverage: float = None,
        verbose: bool = False,
    ) -> None:
        super().__init__(universe, verbose=verbose)
        self.asset_count = len(universe)
        self.weights = self._get_variable(shape=self.asset_count)
        self.long_only = long_only
        self.leverage = leverage if leverage is not None else 1.0
        self.allocations = None

    def add_weight_constraint(
        self,
        min_weights: Union[float, np.array],
        max_weights: Union[float, np.array] = None,
    ) -> None:
        """
        Add weight constraint to optimizer
        weight has to be bigger than min_weights and smaller than max_weights

        Parameters
        ----------
        min_weights: float | np.array
            lower bound for weights
        max_weights: float | np.array, optional
            upper bound for weights
        """
        self._add_constraint(self.weights + 1e-6 >= min_weights)
        if max_weights is not None:
            self._add_constraint(self.weights + 1e-6 <= max_weights)

    def _solve(self) -> None:
        """
        Solve the problem by optimizing the objective function using the constraints
        save optimized weights in self.allocations
        """
        super()._solve()
        solved_weights = (
            np.array(self.weights.value).round(16) + 0.0
        )  # +0.0 removes signed zero
        self.allocations = tuple(
            np.abs(solved_weights) / (np.sum(np.abs(solved_weights)) * self.leverage)
        )
