import cvxpy as cvx
from .base import BaseOptimizer
import quantkit.utils.logging as logging
import numpy as np


class CVXPYOptimizer(BaseOptimizer):
    """
    Contains wrapper methods around cvxpy for building an optimizer.

    Parameters
    ----------
    universe: list, optional
        list of available universe
    verbose: bool, optional
        verbose flag for solver
    """

    def __init__(self, universe: list = None, verbose: bool = False) -> None:
        super().__init__(verbose)
        self.universe = universe
        self._problem = None
        self._solver_options = dict()
        self._solvers = ["ECOS", "SCS", "OSQP", "CVXOPT"]

    @staticmethod
    def _get_variable(shape=(), **kwargs) -> cvx.Variable:
        """
        Create cvxpy variable

        Parameters
        ----------
        shape: optional
            shape of variable

        Returns
        -------
        cvx.Variable
            cvxpy variable
        """
        return cvx.Variable(shape=shape, **kwargs)

    @staticmethod
    def _get_parameter(shape=(), **kwargs) -> cvx.Parameter:
        """
        Create cvxpy parameter

        Parameters
        ----------
        shape: optional
            shape of variable

        Returns
        -------
        cvx.Parameter
            cvxpy parameter
        """
        return cvx.Parameter(shape=shape, **kwargs)

    @staticmethod
    def _minimize(objective):
        """
        Add minimize objective to optimization

        Parameters
        ----------
        objective: function
            objective function

        Returns
        -------
        cvx.Minimize
            cvxpy minimize objective
        """
        return cvx.Minimize(objective)

    @staticmethod
    def _maximize(objective):
        """
        Add maximize objective to optimization

        Parameters
        ----------
        objective: function
            objective function

        Returns
        -------
        cvx.Maximize
            cvxpy maximize objective
        """
        return cvx.Maximize(objective)

    @staticmethod
    def _sum(x: np.ndarray) -> np.ndarray:
        """
        Parameters
        ----------
        x: np.array
            array of values

        Returns
        -------
        np.array
            sum of array
        """
        return cvx.sum(x)

    @staticmethod
    def _sqrt(x: np.ndarray) -> np.ndarray:
        """
        Parameters
        ----------
        x: np.array
            array of values

        Returns
        -------
        np.array
            element wise square root of array
        """
        return cvx.sqrt(x)

    @staticmethod
    def _multiply(x: np.ndarray, y: np.ndarray) -> np.ndarray:
        """
        Parameters
        ----------
        x: np.array
            array of values
        y: np.array
            array of values

        Returns
        -------
        np.array
            element wise product of arrays
        """
        return cvx.multiply(x, y)

    @staticmethod
    def _quad_form(w: np.ndarray, X: np.ndarray) -> np.ndarray:
        """
        Parameters
        ----------
        w: np.array
            array of values (weights)
        X: np.array
            matrix

        Returns
        -------
        np.array
            quadratic form of w and X
        """
        return cvx.quad_form(w, X)

    @staticmethod
    def _norm(X: np.ndarray) -> np.ndarray:
        """
        Parameters
        ----------
        X: np.array
            matrix

        Returns
        -------
        float
            norm of X
        """
        return cvx.norm(X)

    @staticmethod
    def _log(x: np.ndarray) -> np.ndarray:
        """
        Parameters
        ----------
        x: np.array
            array of values

        Returns
        -------
        np.array
            element wise logarithm of array
        """
        return cvx.log(x)

    def solve_problem(self):
        """
        Forming and solving the optimization problem
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

                this_solver_ix += 1

        if self._problem.status not in {"optimal", "optimal_inaccurate"}:
            raise RuntimeError("Solver status: {}".format(self._problem.status))
