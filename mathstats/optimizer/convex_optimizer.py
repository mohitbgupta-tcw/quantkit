import cvxpy as cvx
from .base import BaseOptimizer
import quantkit.utils.logging as logging


class CVXPYOptimizer(BaseOptimizer):
    """
    Contains wrapper methods around cvxpy for building an optimizer.

    Parameters
    ----------
    universe: optional
    optimize_attr: optional
    verbose: optional
    """

    def __init__(self, universe=None, optimize_attr=None, verbose=False):
        super().__init__(verbose)
        self.universe = universe
        self.optimize_attr = optimize_attr
        self._problem = None
        self._solver_options = dict()
        self._solvers = ["ECOS", "SCS", "OSQP", "CVXOPT"]
        # Parameters for Solver
        # 'ECOS': {"max_iters": 500, "abstol": 1e-8},
        # 'SCS': {"max_iters": 2500, "eps": 1e-5},
        # 'OSQP': {"max_iter": 10000, "eps_abs": 1e-8},
        # 'CVXOPT': {"max_iters": 500, "abstol": 1e-8},

    @staticmethod
    def _get_variable(shape=(), **kwargs):
        return cvx.Variable(shape=shape, **kwargs)

    @staticmethod
    def _get_parameter(shape=(), **kwargs):
        return cvx.Parameter(shape=shape, **kwargs)

    @staticmethod
    def _minimize(objective):
        return cvx.Minimize(objective)

    @staticmethod
    def _maximize(objective):
        return cvx.Maximize(objective)

    @staticmethod
    def _sum(x):
        return cvx.sum(x)

    @staticmethod
    def _sqrt(x):
        return cvx.sqrt(x)

    @staticmethod
    def _multiply(x, y):
        return cvx.multiply(x, y)

    @staticmethod
    def _quad_form(w, X):
        return cvx.quad_form(w, X)

    @staticmethod
    def _norm(X):
        return cvx.norm(X)

    @staticmethod
    def _log(x):
        return cvx.log(x)

    def add_variables(self, start_params):
        self.paramters = self._get_variable(start_params.shape)

    def solve_problem(self):
        """
        Forming and solving problem
        """
        self._problem = cvx.Problem(self._objective, self._constraints)
        self._solve()

    def _solve(self):
        """
        Helper method to solve the cvxpy problem and check output,
        once objectives and constraints have been defined
        """
        solved = False
        this_solver_ix = 0
        while not solved:
            try:
                self._problem.solve(
                    solver=self._solvers[this_solver_ix],
                    verbose=self._verbose,
                    **self._solver_options,
                )
                solved = True

            except (TypeError, cvx.DCPError, cvx.SolverError) as e:
                if this_solver_ix == len(self._solvers):
                    raise RuntimeError(e)

                logging.log(f"Solver FAILED {e}")
                this_solver_ix += 1

        if self._problem.status not in {"optimal", "optimal_inaccurate"}:
            raise RuntimeError("Solver status: {}".format(self._problem.status))
