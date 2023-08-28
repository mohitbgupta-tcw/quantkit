import numpy as np


class BaseOptimizer(object):
    """
    Contains necessary optimization calculations

    Parameter
    ---------
    verbose: bool
        verbose flag for solver
    """

    MAX_ITER = 300

    def __init__(self, verbose=False) -> None:
        self._objective = None
        self._constraints = []
        self._verbose = verbose
        self._optimized_fun = None
        self.success = False

    @property
    def optimized_fun(self):
        return self._optimized_fun

    def add_objective(self, obj) -> None:
        """
        Add objective function to optimization

        Parameters
        ----------
        obj:
            objective
        """
        assert callable(obj)
        self._objective = obj

    def _add_constraint(self, new_constraint) -> None:
        """
        Add constraint to optimization
        """
        self._constraints.append(new_constraint)

    @staticmethod
    def _sum(x) -> np.array:
        return sum(x)

    @staticmethod
    def _sqrt(x) -> np.array:
        return np.sqrt(x)

    @staticmethod
    def _multiply(x, y) -> np.array:
        return np.multiply(x, y)

    @staticmethod
    def _log(x) -> np.array:
        return np.log(x)

    @staticmethod
    def _quad_form(w, X) -> np.array:
        return w.T @ X @ w

    @staticmethod
    def _norm(X) -> np.array:
        return np.linalg.norm(X)

    def update_params(self):
        raise NotImplementedError()

    def solve_problem(self, tolerance=1e-6) -> np.array:
        """
        Parameters
        ----------
        tolerance: float
            convergence tolerance
        """
        _continue = True
        prior_obj_val = None
        counter = 0
        while _continue:
            this_obj_val = self._objective()
            if np.isnan(this_obj_val):
                raise RuntimeError(f"invalid objective value {this_obj_val}")

            if (
                prior_obj_val is not None
                and this_obj_val - prior_obj_val <= abs(this_obj_val) * tolerance
            ):
                _continue = False
            else:
                prior_obj_val = this_obj_val
                self.update_params()

            # Error check
            counter += 1
            if counter > self.MAX_ITER:
                raise RuntimeError(f"max iteration {self.MAX_ITER} reached")

        return prior_obj_val
