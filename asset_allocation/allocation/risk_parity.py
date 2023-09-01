import quantkit.asset_allocation.allocation.portfolio_optimizer as portfolio_optimizer
import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import numpy as np


class TraditionalRPOptimizer(portfolio_optimizer.PortfolioOptimizer):
    """Use traditional risk parity formulation
    Reference:
    S. Maillard, T. Roncalli, and J. Teiletche, "The properties of equally weighted risk contribution portfolios"
    and
    B. Bruder and T. Roncalli, "Managing risk exposures using the risk budgeting approach"
    """

    def __init__(
        self,
        universe,
        optimize_attr,
        risk_budgets,
        long_only=True,
        leverage=None,
        verbose=False,
    ):
        """
        Parameters
        ----------
        universe: list
            investment universe
        optimize_attr: list
            attribute to run optimization on
        risk_budgets, np.array
            amount of total risk each asset can take in final portfolio
        long_only: bool, optional
            allow long only portfolio or add short positions
        leverage: float, optional
            portfolio leverage
        verbose: bool, optional
            verbose flag for solver
        """
        super().__init__(universe, optimize_attr, long_only, leverage, verbose=verbose)
        self.risk_budgets = risk_budgets
        # PSD: positive semi-definite
        self.cov_matrix = self._get_parameter(
            shape=(self.asset_count, self.asset_count), PSD=True
        )
        # Having theta to save computational complexity
        self.theta = self._get_variable(nonneg=True)

        self.add_objective()
        self.add_constraints()

    def add_objective(self):
        r"""
        Reference: Roncalli, Richard (2019) Constrained Risk Budgeting Portfolios Theory, Algorithms, Applications & Puzzles

        objective function:
            argmin_w (0.5*w^T\Sigma w - b*log(w))
        """
        risk_term = 0.5 * self._quad_form(self.weights, self.cov_matrix)
        log_term = self.risk_budgets @ self._log(self.weights)
        self._objective = self._minimize(risk_term - log_term)
        return

    def add_constraints(self):
        r"""
        Add constraints to optimizer:
            - weight constraint: w >= 0
        """
        self.add_weight_constraint(min_weights=0.0)
        # TODO Change sum of weight to leverage
        # self._add_constraint(self._sum(self.weights)==1)
        return


class RiskParity(allocation_base.Allocation):
    """Risk Parity can be formulated to a convex function:
    https://mirca.github.io/riskparity.py/
    Jean-Charles Richard and Thierry Roncalli. Constrained Risk Budgeting Portfolios: Theory, Algorithms, Applications & Puzzles
    """

    def __init__(
        self,
        asset_list,
        risk_engine,
        return_engine,
        portfolio_leverage=1.0,
        verbose=False,
        **kwargs
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
        portfolio_leverage: float, optional
            portfolio leverage
        verbose: bool, optional
            verbose flag for solver
        """
        super().__init__(asset_list, risk_engine, return_engine)
        self.risk_budgets = np.ones(self.asset_count) / self.asset_count
        self.c_scalar = 1.0
        self.optimizer = TraditionalRPOptimizer(
            universe=asset_list,
            optimize_attr=asset_list,
            risk_budgets=self.risk_budgets,
            leverage=portfolio_leverage,
            verbose=verbose,
        )

    def update(self, risk_budgets=None):
        """
        Assign new forecasted cov matrix from risk engine to optimizer

        Parameters
        ----------
        risk_budgets, np.array, optional
            amount of total risk each asset can take in final portfolio
        """
        risk_metrics = self.risk_engine.risk_metrics_optimizer
        assert len(risk_metrics) == self.asset_count
        self.optimizer.cov_matrix.value = risk_metrics
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
