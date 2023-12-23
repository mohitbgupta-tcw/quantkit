import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import quantkit.mathstats.matrix.correlation as correlation
import quantkit.mathstats.matrix.distance as distance
import quantkit.mathstats.matrix.diagonalization as diagonalization
import quantkit.mathstats.matrix.variance as variance
import quantkit.asset_allocation.allocation.portfolio_optimizer as portfolio_optimizer
import quantkit.utils.util_functions as util_functions
import quantkit.asset_allocation.risk_management.allocation_limit.group_limit as group_limit
import numpy as np
from typing import Union
import datetime


class HRPOptimizer(portfolio_optimizer.PortfolioOptimizer):
    r"""
    Base class to calculate Hierarchical Risk Parity (RP) weighting scheme
    Reference:
    - M. L. de Prado, "Building Diversified Portfolios that Outperform Out-of-Sample"


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
    long_only: bool, optional
        allow long only portfolio or add short positions
    leverage: float, optional
        portfolio leverage
    verbose: bool, optional
        verbose flag for solver
    """

    def __init__(
        self,
        universe: list,
        cov_matrix: np.ndarray,
        min_weights: Union[float, np.ndarray] = 0.0,
        max_weights: Union[float, np.ndarray] = 1.0,
        long_only: bool = True,
        leverage: float = None,
        verbose: bool = False,
    ) -> None:
        super().__init__(universe, long_only, leverage, verbose=verbose)
        self.cov_matrix = cov_matrix
        self.min_weights = min_weights
        self.max_weights = max_weights

        self.add_objective()
        self.add_constraints()

    def add_objective(self) -> None:
        r"""
        Objective Function for HRP
        """
        pass

    def add_constraints(self) -> None:
        r"""
        Constraints for HRP

        Math
        ----
            - min_weight <= weight <= max_weight
        """
        self.add_weight_constraint(
            min_weights=self.min_weights, max_weights=self.max_weights
        )
        # self._add_constraint(self._sum(self.weights)==1)

    def solve_problem(self):
        """
        Forming and solving the optimization problem
        """
        corr = correlation.cov_to_corr(self.cov_matrix)
        dist = distance.distance_matrix(corr)
        link = distance.linkage_matrix(dist)
        cluster_items = [diagonalization.get_quasi_diag(link)]
        weights = np.ones(shape=self.asset_count)

        while len(cluster_items) > 0:
            cluster_items = util_functions.bisect_list(cluster_items)
            for i in range(0, len(cluster_items), 2):
                first_cluster = cluster_items[i]
                second_cluster = cluster_items[i + 1]
                first_variance = variance.sliced_inverse_variance(
                    self.cov_matrix, first_cluster
                )
                second_variance = variance.sliced_inverse_variance(
                    self.cov_matrix, second_cluster
                )
                alpha = 1 - first_variance / (first_variance + second_variance)
                alpha = min(
                    sum(self.max_weights[first_cluster]) / weights[first_cluster[0]],
                    max(
                        sum(self.min_weights[first_cluster])
                        / weights[first_cluster[0]],
                        alpha,
                    ),
                )
                alpha = 1 - min(
                    sum(self.max_weights[second_cluster]) / weights[second_cluster[0]],
                    max(
                        sum(self.min_weights[second_cluster])
                        / weights[second_cluster][0],
                        1 - alpha,
                    ),
                )
                weights[first_cluster] *= alpha
                weights[second_cluster] *= 1 - alpha
        weights = weights.round(16) + 0.0
        self.allocations = tuple(
            np.abs(weights) / (np.sum(np.abs(weights)) * self.leverage)
        )


class HierarchicalRiskParity(allocation_base.Allocation):
    r"""
    Base class to calculate Hierarchical Risk Parity (HRP) weighting scheme
    Reference:
    - M. L. de Prado, "Building Diversified Portfolios that Outperform Out-of-Sample"

    Calculation
    -----------
    1) Correlation Matrix
    2) Distance Matrix:
        D = \sqrt{\frac{1}{2} (1-Cor(X)}
    3) Euclidean Distance
    4) Quasi-Diagonalization
    5) Bisection
    6) Weight Calculation


    Parameters
    ----------
    asset_list: list
        all assets to run optimization on
    risk_engine: asset_allocation.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    return_engine: asset_allocation.return_calc.return_metrics
        return engine used to forecast returns
    portfolio_leverage: float, optional
        portfolio leverage
    verbose: bool, optional
        verbose flag for solver
    scaling: dict, optional
        dictionary to scale assets, must have the following components:
        {
            "limited_assets": [],
            "limit": 0.35,
            "allocate_to": []
        }
    """

    def __init__(
        self,
        asset_list: list,
        risk_engine,
        return_engine,
        portfolio_leverage: float = 1.0,
        verbose: bool = False,
        weights_constraint: dict = None,
        scaling: dict = None,
        **kwargs,
    ) -> None:
        super().__init__(asset_list, risk_engine, return_engine)
        self.risk_budgets = np.ones(self.num_total_assets) / self.num_total_assets
        self.c_scalar = 1.0
        self.portfolio_leverage = portfolio_leverage
        self.verbose = verbose
        self.min_weights, self.max_weights = self.get_weights_constraints(
            weights_constraint
        )
        self.scaling = scaling

    def update(
        self,
        selected_assets: Union[list, np.ndarray],
        **kwargs,
    ) -> None:
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
        self.optimizer = HRPOptimizer(
            universe=selected_assets,
            cov_matrix=risk_metrics,
            min_weights=self.min_weights[selected_assets],
            max_weights=self.max_weights[selected_assets],
            leverage=self.portfolio_leverage,
            verbose=self.verbose,
        )

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

        if self.scaling:
            allocation = group_limit.limit_group(
                weights=allocation,
                universe=self.asset_list,
                max_allocation=self.max_weights,
                **self.scaling,
            )

        self.allocations = (date, allocation)
        self.allocations_history[date] = allocation
