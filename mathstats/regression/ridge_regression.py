import numpy as np
import quantkit.mathstats.regression.ols_regression as ols_regression


class RidgeLR(ols_regression.OrdinaryLR):
    r"""
    A class for performing Ridge Regression on windows of streaming data
    Calculates online version of sklearn.linear_model.Ridge()

    Model
    -----
    $$ dep_var = beta * ind_vars + mathcal{N}(0, sigma) + L2_regularization$$

    Calculation
    -----------
    The standard least-square fit gives regression coefficients

    beta = (X'X + alpha I)^{-1}X'Y
    (see https://stats.stackexchange.com/a/602415)

    Parameters
    ----------
    num_ind_variables : int
        Total number of independent variables to be used in regression
    num_dep_variables : int
        Total number of dependent variables to be used in regression
    window_size : int, optional
        window size of the rolling regression
    alpha: float, optional
        ridge paramater
    """

    def __init__(
        self,
        num_ind_variables: int,
        num_dep_variables: int,
        window_size: int = 1,
        alpha: float = 1,
    ) -> None:
        super().__init__(num_ind_variables, num_dep_variables, window_size)
        self.alpha = alpha

    def calculate_vector(self, mask_current: np.array) -> np.array:
        """
        Calculate regression specific vector

        Parameters
        ---------
        mask_current: np.array
            array indicating which positions are nan's

        Returns
        -------
        np.array
            vector
        """
        if self.wxx.matrix.shape[0] == 1:
            _S_wxx_inv = 1 / self.wxx.curr_vector + self.alpha
            m = (_S_wxx_inv * self.wxy.curr_vector) * mask_current
        else:
            _Identity = self.alpha * np.identity(self.num_ind_variables + 1)
            _Identity[0, 0] = 0
            _S_wxx_inv = np.linalg.pinv(self.wxx.curr_vector + _Identity)
            m = (_S_wxx_inv @ self.wxy.curr_vector) * mask_current
        return m
