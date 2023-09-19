import numpy as np
import quantkit.mathstats.regression.ols_regression as ols_regression
import quantkit.mathstats.streaming_base.window_base as window_base


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
    (see https://en.wikipedia.org/wiki/Ridge_regression)

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
            # _Identity = self.alpha * np.identity(self.num_ind_variables+1)
            _S_wxx_inv = np.linalg.pinv(self.wxx.curr_vector)
            m = (_S_wxx_inv @ self.wxy.curr_vector) * mask_current
        return m

    def update(
        self,
        batch_ind: np.ndarray,
        batch_dep: np.ndarray,
        batch_weight: float = 1,
    ) -> None:
        """
        Update cov caclulator and rolling regression with new dependent and independent variable data

        Parameters
        ----------
        batch_ind : (1, number_ind_variables) numpy array
            The independent variables in this batch
        batch_dep : (number_of_dep_variables, 1) numpy array
            All of the dependant variables we want to regress on in this batch
        batch_weight : float, default 1
            The weight for this batch
        """
        batch_dep = batch_dep[np.newaxis, :]
        batch_ind = batch_ind[np.newaxis, :]
        _all_batch = np.append(batch_ind, batch_dep, 1).squeeze()
        self.cov_calculator.update(_all_batch)

        self.total_iterations += 1
        batch_ind = np.insert(batch_ind, 0, 1, axis=1)

        _s_wxy_new = batch_dep * batch_ind.T * batch_weight
        _S_wxx_new = batch_ind.T @ batch_ind * batch_weight + self.alpha * np.identity(
            self.num_ind_variables + 1
        )
        _mask_new = np.where(np.isnan(batch_dep), 0, 1)

        self.wxy.update(new_vector=_s_wxy_new)
        self.wxx.update(new_vector=_S_wxx_new)
        self._mask.update(new_vector=_mask_new)
