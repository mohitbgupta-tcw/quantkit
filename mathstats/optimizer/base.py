import numpy as np


class BaseOptimizer(object):
    """
    Contains necessary calculations for optimizations

    Parameters
    ----------
    verbose: bool, optional
        verbose flag for solver
    """

    MAX_ITER = 300

    def __init__(self, verbose: bool = False) -> None:
        self._objective = None
        self._constraints = []
        self._verbose = verbose
        self.success = False

    def add_objective(self, obj) -> None:
        """
        Add objective function to optimization

        Parameters
        ----------
        obj: function
            objective function
        """
        assert callable(obj)
        self._objective = obj

    def _add_constraint(self, new_constraint) -> None:
        """
        Add constraint to optimization

        Parameters
        ----------
        new_constrainer: function
            constraint function
        """
        self._constraints.append(new_constraint)

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
        return sum(x)

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
        return np.sqrt(x)

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
        return np.multiply(x, y)

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
        return np.log(x)

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
        return w.T @ X @ w

    @staticmethod
    def _norm(X: np.ndarray) -> float:
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
        return np.linalg.norm(X)
