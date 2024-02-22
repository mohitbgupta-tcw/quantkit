import quantkit.backtester.allocation.portfolio_optimizer as portfolio_optimizer
import quantkit.backtester.allocation.allocation_base as allocation_base
import pandas as pd
import numpy as np
import cvxpy as cvx
from typing import Union
import datetime


class MeanVarianceOptimizer(portfolio_optimizer.PortfolioOptimizer):
    r"""
    Base class to calculate Mean Variance Optimization (MVO) weighting scheme
    Reference: H. Markowitz, "Portfolio Selection"

    Calculation
    -----------
        argmax_w (w^T*R - \lambda w^T\Sigma w)

        s.t.
        min_weight <= weight <= max_weight
        sum(weight) = 1

    Parameters
    ----------
    universe: list
        investment universe
    cov_matrix: np.array
        covariance matrix
    risk_averse_lambda: float, optional
        lambda determining weighting between return and risk
    min_weights: float | np.array
        lower bound for weights
    max_weights: float | list, np.array
        upper bound for weights
    leverage: float, optional
        portfolio leverage, if leverage is None, solve for optimal leverage
    """

    def __init__(
        self,
        universe: list,
        cov_matrix: np.ndarray,
        risk_averse_lambda: float = 1.0,
        min_weights: Union[float, np.ndarray] = 0.0,
        max_weights: Union[float, np.ndarray] = 1.0,
        leverage: float = None,
    ) -> None:
        super().__init__(universe)
        # PSD: positive semi-definite
        self.cov_matrix = cvx.atoms.affine.wraps.psd_wrap(cov_matrix)
        self.exp_returns = self._get_parameter(shape=self.asset_count)
        self.risk_averse_lambda = self._get_parameter(
            value=risk_averse_lambda, pos=True
        )
        self.min_weights = min_weights
        self.max_weights = max_weights
        self.add_objective()
        self.add_constraints()

    def add_objective(self) -> None:
        r"""
        Objective Function for MVO

        Math
        ----
        argmax_w (w^T*R - \lambda w^T\Sigma w)
        """
        return_term = self.weights.T @ self.exp_returns
        risk_term = self._quad_form(self.weights, self.cov_matrix)
        self._objective = self._maximize(
            return_term - self.risk_averse_lambda * risk_term
        )

    def add_constraints(self) -> None:
        r"""
        Constraints for MVO

        Math
        ----
            - min_weight <= weight <= max_weight
            - sum(weight) = 1
        """
        self.add_weight_constraint(
            min_weights=self.min_weights, max_weights=self.max_weights
        )
        self._add_constraint(self._sum(self.weights) == 1)


class MeanVariance(allocation_base.Allocation):
    r"""
    Base class to calculate Mean Variance Optimization (MVO) weighting scheme
    Reference: H. Markowitz, "Portfolio Selection"

    Calculation
    -----------
        argmax_w (w^T*R - \lambda w^T\Sigma w)

        s.t.
        min_weight <= weight <= max_weight
        sum(weight) = 1

    Parameters
    ----------
    asset_list: list
        all assets to run optimization on
    risk_engine: backtester.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    return_engine: backtester.return_calc.return_metrics
        return engine used to forecast returns
    weight_constraint: dict
        dictionary of weight_constraints
    portfolio_leverage: float, optional
        portfolio leverage
    """

    def __init__(
        self,
        asset_list: list,
        risk_engine,
        return_engine,
        weights_constraint: dict = None,
        portfolio_leverage: float = 1.0,
        **kwargs,
    ) -> None:
        super().__init__(
            asset_list,
            risk_engine,
            return_engine,
            portfolio_leverage=portfolio_leverage,
        )
        self.risk_metrics = pd.DataFrame(
            np.ones((self.num_total_assets, self.num_total_assets)) * np.nan,
            columns=asset_list,
            index=asset_list,
        )
        self.return_metrics = pd.DataFrame(
            np.ones(self.num_total_assets) * np.nan, index=asset_list
        )
        self.min_weights, self.max_weights = self.get_weights_constraints(
            weights_constraint
        )

    def update(self, selected_assets: Union[list, np.ndarray], **kwargs) -> None:
        """
        - initialize optimizer
        - assign new forecasted cov matrix from risk engine to optimizer
        - assign forecasted returns from return engine to optimizer

        Parameters
        ----------
        selected_assets: list | np.array
            list of selected assets (their location in universe as integer)
        """
        risk_metrics = self.risk_engine.risk_metrics_optimizer
        risk_metrics = risk_metrics[np.ix_(selected_assets, selected_assets)]
        return_metrics = self.return_engine.return_metrics_intuitive
        return_metrics = return_metrics[selected_assets]

        self.optimizer = MeanVarianceOptimizer(
            universe=selected_assets,
            cov_matrix=risk_metrics,
            min_weights=self.min_weights[selected_assets],
            max_weights=self.max_weights[selected_assets],
        )

        self.optimizer.exp_returns.value = return_metrics

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
