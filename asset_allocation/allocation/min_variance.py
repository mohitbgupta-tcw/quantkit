import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import quantkit.asset_allocation.allocation.portfolio_optimizer as portfolio_optimizer
import numpy as np
import scipy as sp
import cvxpy as cvx
from typing import Union
import datetime


class MinVarianceOptimizer(portfolio_optimizer.PortfolioOptimizer):
    r"""
    Base class to calculate Minimum Variance Optimization weighting scheme
    Reference: https://quant.stackexchange.com/questions/69278/minimum-variance-portfolio-in-python

    Calculation
    -----------
        argmin_w (1/2 w^T\Sigma w)

        s.t.
        min_weight <= weight <= max_weight
        sum(weight) = 1

    Parameters
    ----------
    universe: list
        investment universe
    cov_matrix: np.array
        covariance matrix
    min_weights: float | np.array
        lower bound for weights
    max_weights: float | list, np.array
        upper bound for weights
    """

    def __init__(
        self,
        universe: list,
        cov_matrix: np.ndarray,
        min_weights: Union[float, np.ndarray] = 0.0,
        max_weights: Union[float, np.ndarray] = 1.0,
    ) -> None:
        super().__init__(universe)
        # PSD: positive semi-definite
        self.cov_matrix = cvx.atoms.affine.wraps.psd_wrap(cov_matrix)
        self.min_weights = min_weights
        self.max_weights = max_weights
        self.add_objective()
        self.add_constraints()

    def add_objective(self) -> None:
        r"""
        Objective Function for Min Variance Optimization

        Math
        ----
        argmin_w (1/2 w^T\Sigma w)
        """
        risk_term = 0.5 * self._quad_form(self.weights, self.cov_matrix)
        self._objective = self._minimize(risk_term)

    def add_constraints(self) -> None:
        r"""
        Constraints for Min Variance Optimization

        Math
        ----
            - min_weight <= weight <= max_weight
            - sum(weight) = 1
        """
        self.add_weight_constraint(
            min_weights=self.min_weights, max_weights=self.max_weights
        )
        self._add_constraint(self._sum(self.weights) == 1)


class MinimumVariance(allocation_base.Allocation):
    r"""
    Base class to calculate Minimum Variance Optimization weighting scheme
    Reference: https://quant.stackexchange.com/questions/69278/minimum-variance-portfolio-in-python

    Calculation
    -----------
        argmin_w (1/2 w^T\Sigma w)

        s.t.
        min_weight <= weight <= max_weight
        sum(weight) = 1

    Parameters
    ----------
    asset_list: list
        assets to run optimization on
    risk_engine: asset_allocation.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    return_engine: asset_allocation.return_calc.return_metrics
        return engine used to forecast returns
    weight_constraint: dict
        dictionary of weight_constraints
    """

    def __init__(
        self,
        asset_list: list,
        risk_engine,
        return_engine,
        weights_constraint: dict = None,
        **kwargs,
    ) -> None:
        super().__init__(asset_list, risk_engine, return_engine)
        self.min_weights, self.max_weights = self.get_weights_constraints(
            weights_constraint
        )
        self.risk_metrics = np.ones(self.num_total_assets) * np.nan
        self.allocations = np.ones(self.num_total_assets) * np.nan

    def update(self, selected_assets: Union[list, np.ndarray], **kwargs) -> None:
        """
        - initialize optimizer
        - assign new forecasted cov matrix from risk engine to optimizer

        Parameters
        ----------
        selected_assets: list | np.array
            list of selected assets (their location in universe as integer)
        """
        risk_metrics = self.risk_engine.risk_metrics_optimizer
        risk_metrics = risk_metrics[np.ix_(selected_assets, selected_assets)]
        self.optimizer = MinVarianceOptimizer(
            universe=selected_assets,
            cov_matrix=risk_metrics,
            min_weights=self.min_weights[selected_assets],
            max_weights=self.max_weights[selected_assets],
        )

    def minimize_portfolio_variance(self, risk_metrics) -> np.ndarray:
        r"""
        Minimum variance optimization in case of no existing short constraint

        Math
        ----
        Lagrangian of the optimization problem
        L(\mathbf{w},\lambda) = \mathbf{w}'\Sigma\mathbf{w} + \lambda(\mathbf{w}'\mathbf{1} - 1)

        \mathbf{w} = \frac{\Sigma^{-1}\mathbf{1}}{\mathbf{1}'\Sigma^{-1}\mathbf{1}}

        Returns
        -------
        np.array
            array of optimal weights
        """
        inv_cov = sp.linalg.inv(risk_metrics)
        return np.sum(inv_cov, axis=0) / np.sum(inv_cov)

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
        self.optimizer.solve_problem()
        opt_allocation = self.optimizer.allocations
        allocation[selected_assets] = opt_allocation

        self.allocations = (date, allocation)
        self.allocations_history[date] = allocation
