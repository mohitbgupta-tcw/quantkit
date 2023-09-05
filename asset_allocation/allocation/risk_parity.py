import quantkit.asset_allocation.allocation.portfolio_optimizer as portfolio_optimizer
import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import numpy as np
import cvxpy as cvx
from typing import Union


class TraditionalRPOptimizer(portfolio_optimizer.PortfolioOptimizer):
    r"""
    Base class to calculate Risk Parity (RP) weighting scheme
    Reference:
    - S. Maillard, T. Roncalli, and J. Teiletche, "The properties of equally weighted risk contribution portfolios"
    - B. Bruder and T. Roncalli, "Managing risk exposures using the risk budgeting approach"

    Calculation
    -----------
        argmin_w (0.5*w^T\Sigma w - b*log(w))

        s.t.
        w >= 0

    Parameters
    ----------
    universe: list
        investment universe
    cov_matrix: np.array
        covariance matrix
    risk_budgets, np.array
        amount of total risk each asset can take in final portfolio
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
        cov_matrix: np.array,
        risk_budgets: np.array,
        long_only: bool = True,
        leverage: float = None,
        verbose: bool = False,
    ) -> None:
        super().__init__(universe, long_only, leverage, verbose=verbose)
        self.risk_budgets = risk_budgets
        # PSD: positive semi-definite
        self.cov_matrix = cvx.atoms.affine.wraps.psd_wrap(cov_matrix)
        # Having theta to save computational complexity
        self.theta = self._get_variable(nonneg=True)

        self.add_objective()
        self.add_constraints()

    def add_objective(self) -> None:
        r"""
        Objective Function for RP
        Reference: Roncalli, Richard (2019) Constrained Risk Budgeting Portfolios Theory, Algorithms, Applications & Puzzles

        Math
        ----
        argmin_w (0.5*w^T\Sigma w - b*log(w))
        """
        risk_term = 0.5 * self._quad_form(self.weights, self.cov_matrix)
        log_term = self.risk_budgets @ self._log(self.weights)
        self._objective = self._minimize(risk_term - log_term)

    def add_constraints(self) -> None:
        r"""
        Constraints for RP

        Math
        ----
            - weight constraint: w >= 0
        """
        self.add_weight_constraint(min_weights=0.0)
        # self._add_constraint(self._sum(self.weights)==1)


class RiskParity(allocation_base.Allocation):
    r"""
    Base class to calculate Risk Parity (RP) weighting scheme
    Reference:
    - S. Maillard, T. Roncalli, and J. Teiletche, "The properties of equally weighted risk contribution portfolios"
    - B. Bruder and T. Roncalli, "Managing risk exposures using the risk budgeting approach"

    Calculation
    -----------
        argmin_w (0.5*w^T\Sigma w - b*log(w))

        s.t.
        w >= 0

    Parameters
    ----------
    asset_list: list
        all assets to run optimization on
    risk_engine: mstar_asset_allocation.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    return_engine: mstar_asset_allocation.return_calc.return_metrics
        return engine used to forecast returns
    portfolio_leverage: float, optional
        portfolio leverage
    verbose: bool, optional
        verbose flag for solver
    """

    def __init__(
        self,
        asset_list: list,
        risk_engine,
        return_engine,
        portfolio_leverage: float = 1.0,
        verbose: bool = False,
        **kwargs
    ) -> None:
        super().__init__(asset_list, risk_engine, return_engine)
        self.risk_budgets = np.ones(self.num_total_assets) / self.num_total_assets
        self.c_scalar = 1.0
        self.portfolio_leverage = portfolio_leverage
        self.verbose = verbose

    def update(
        self,
        selected_assets: Union[list, np.array],
        risk_budgets: np.array = None,
        **kwargs
    ) -> None:
        """
        - initialize optimizer
        - assign new forecasted cov matrix from risk engine to optimizer

        Parameters
        ----------
        selected_assets: list | np.array
            list of selected assets (their location in universe as integer)
        risk_budgets, np.array, optional
            amount of total risk each asset can take in final portfolio
        """
        risk_metrics = self.risk_engine.risk_metrics_optimizer
        risk_metrics = risk_metrics[np.ix_(selected_assets, selected_assets)]
        risk_budgets = np.ones(len(selected_assets)) / len(selected_assets)

        self.optimizer = TraditionalRPOptimizer(
            universe=selected_assets,
            cov_matrix=risk_metrics,
            risk_budgets=risk_budgets,
            leverage=self.portfolio_leverage,
            verbose=self.verbose,
        )

    def allocate(self, date, selected_assets: Union[list, np.array]) -> None:
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

        self.allocations = ((date,), allocation)
        self.allocations_history.append(self.allocations)
