import quantkit.asset_allocation.allocation.portfolio_optimizer as portfolio_optimizer
import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import pandas as pd
import numpy as np


class MeanVarianceOptimizer(portfolio_optimizer.PortfolioOptimizer):
    """Use traditional mean-variance formulation
    Reference:
    H. Markowitz, "Portfolio Selection"
    """

    def __init__(
        self,
        universe,
        optimize_attr,
        risk_averse_lambda=1.0,
        min_weights=0.0,
        max_weights=1.0,
    ):
        """
        Parameters
        ----------
        universe: list
            investment universe
        optimize_attr: list
            attribute to run optimization on
        risk_averse_lambda: float, optional
            lambda determining weighting between return and risk
        min_weights: float | np.array
            lower bound for weights
        max_weights: float | list, np.array
            upper bound for weights
        """
        super().__init__(universe, optimize_attr)
        # PSD: positive semi-definite
        self.cov_matrix = self._get_parameter(
            shape=(self.asset_count, self.asset_count), PSD=True
        )
        self.exp_returns = self._get_parameter(shape=self.asset_count)
        self.risk_averse_lambda = self._get_parameter(
            value=risk_averse_lambda, pos=True
        )
        self.min_weights = min_weights
        self.max_weights = max_weights
        self.add_objective()
        self.add_constraints()

    def add_objective(self):
        r"""
        objective function:
            argmax_w (w^T*R - \lambda w^T\Sigma w)
        """
        return_term = self.weights.T @ self.exp_returns
        risk_term = self._quad_form(self.weights, self.cov_matrix)
        self._objective = self._maximize(
            return_term - self.risk_averse_lambda * risk_term
        )
        return

    def add_constraints(self):
        r"""
        Add constraints to optimizer:
            - min_weight <= weight <= max_weight
            - sum(weight) = 1
        """
        self.add_weight_constraint(
            min_weights=self.min_weights, max_weights=self.max_weights
        )
        self._add_constraint(self._sum(self.weights) == 1)
        return


class MeanVariance(allocation_base.Allocation):
    def __init__(
        self, asset_list, risk_engine, return_engine, weights_constraint=None, **kwargs
    ):
        """
        Parameters
        ----------
        asset_list: list
            assets to run optimization on
        risk_engine: mstar_asset_allocation.risk_calc.risk_metrics
            risk engine used to forecast cov matrix
        return_engine: mstar_asset_allocation.return_calc.return_metrics
            return engine used to forecast returns
        weight_constraint: dict
            dictionary of weight_constraints
        """
        super().__init__(asset_list, risk_engine, return_engine)
        self.risk_metrics = pd.DataFrame(
            np.ones((self.asset_count, self.asset_count)) * np.nan,
            columns=asset_list,
            index=asset_list,
        )
        self.return_metrics = pd.DataFrame(
            np.ones(self.asset_count) * np.nan, index=asset_list
        )
        min_weights, max_weights = self.get_weights_constraints(weights_constraint)
        self.optimizer = MeanVarianceOptimizer(
            universe=asset_list,
            optimize_attr=asset_list,
            min_weights=min_weights,
            max_weights=max_weights,
        )

    def update(self, **kwargs):
        """
        Assign new forecasted cov matrix from risk engine
        and return from return engine to optimizer
        """
        self.risk_metrics = self.risk_engine.risk_metrics_optimizer
        assert len(self.risk_metrics) == self.asset_count

        self.return_metrics = self.return_engine.return_metrics_optimizer
        assert len(self.return_metrics) == self.asset_count

        self.optimizer.cov_matrix.value = self.risk_metrics
        self.optimizer.exp_returns.value = self.return_metrics
        return

    def allocate(self, date):
        """
        Solve for optimal portfolio and save allocation

        Parameters
        ----------
        date : datetime.date
            date of snapshot
        """
        self.optimizer.solve_problem()
        self.allocations = (date,) + self.optimizer.allocations
        self.allocations_history.append(self.allocations)
        return
