import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import quantkit.asset_allocation.allocation.portfolio_optimizer as portfolio_optimizer
import numpy as np
import scipy as sp
import cvxpy


class MinVarianceOptimizer(portfolio_optimizer.PortfolioOptimizer):
    """
    https://quant.stackexchange.com/questions/69278/minimum-variance-portfolio-in-python
    """

    def __init__(
        self,
        universe,
        cov_matrix,
        min_weights=0.0,
        max_weights=1.0,
    ):
        """
        Parameters
        ----------
        universe: list
            investment universe
        min_weights: float | np.array
            lower bound for weights
        max_weights: float | list, np.array
            upper bound for weights
        """
        super().__init__(universe)
        # PSD: positive semi-definite
        self.cov_matrix = cvxpy.atoms.affine.wraps.psd_wrap(cov_matrix)
        self.min_weights = min_weights
        self.max_weights = max_weights
        self.add_objective()
        self.add_constraints()

    def add_objective(self):
        r"""
        objective function:
            argmin_w (1/2 w^T\Sigma w)
        """
        risk_term = self._quad_form(self.weights, self.cov_matrix)
        self._objective = self._minimize(risk_term)
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


class MinimumVariance(allocation_base.Allocation):
    r"""Analytical solution when there is SHORT constraint

    Math
    ----
    argmin_{w} \sigma_{p,m}^2 = w' \Sigma w
    s.t. m'1 = 1

    """

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
        """
        super().__init__(asset_list, risk_engine, return_engine)
        self.min_weights, self.max_weights = self.get_weights_constraints(
            weights_constraint
        )
        self.risk_metrics = np.ones(self.num_total_assets) * np.nan
        self.allocations = np.ones(self.num_total_assets) * np.nan

    def update(self, selected_assets, **kwargs):
        """
        Assign new forecasted cov matrix from risk engine to risk_metrics
        """
        self.risk_metrics = self.risk_engine.risk_metrics_optimizer
        self.risk_metrics = self.risk_metrics[np.ix_(selected_assets, selected_assets)]
        self.optimizer = MinVarianceOptimizer(
            universe=selected_assets,
            cov_matrix=self.risk_metrics,
            min_weights=self.min_weights[selected_assets],
            max_weights=self.max_weights[selected_assets],
        )
        return

    def minimize_portfolio_variance(self, risk_metrics):
        r"""
        For no short constraint calculation
        Math
        ----
        Lagrangian of the optimization problem
        L(\mathbf{w},\lambda) = \mathbf{w}'\Sigma\mathbf{w} + \lambda(\mathbf{w}'\mathbf{1} - 1)

        \mathbf{w} = \frac{\Sigma^{-1}\mathbf{1}}{\mathbf{1}'\Sigma^{-1}\mathbf{1}}
        """
        inv_cov = sp.linalg.inv(risk_metrics)
        return np.sum(inv_cov, axis=0) / np.sum(inv_cov)

    def allocate(self, date, selected_assets):
        """
        Solve for optimal portfolio and save allocation

        Parameters
        ----------
        date : datetime.date
            date of snapshot
        """
        allocation = np.zeros(shape=self.num_total_assets)
        self.optimizer.solve_problem()
        opt_allocation = self.optimizer.allocations
        allocation[selected_assets] = opt_allocation

        self.allocations = ((date,), allocation)
        self.allocations_history.append(self.allocations)
        return
