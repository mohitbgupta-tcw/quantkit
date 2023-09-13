import quantkit.mathstats.streaming_base.streaming_base as streaming_base
import quantkit.mathstats.streaming_base.window_base as window_base
import numpy as np


class WindowLS(streaming_base.StreamingBase):
    """
    A class for performing least squares regression on windows of streaming data.

    Attributes
    ----------
    total_iterations: int
        number of update calls
    wyy: np.ndarray
        for internal computation of regression parameters
    wxy: np.ndarray
        for internal computation of regression parameters
    wxx: np.ndarray
        for internal computation of regression parameters
    number_of_dep_variables: int
        number of independent variables
    number_of_ind_variables:
        number of dependant variables
    results: dict
        beta
        sigma
        tstat

    Methods
    -------
    update:
        updates the WLS estimates based on the new row of data

    Notes
    -----
    Using multiple dependant variable is the same as running the regression separately on with each dependant variable individually.

    Model:
    $$ dep_var = beta ind_vars + mathcal{N}(0, sigma)$$
    """

    def __init__(
        self,
        num_ind_variables: int,
        num_dep_variables: int,
        window_size: int = 1,
    ) -> None:
        """
        Parameters
        ----------
        num_ind_variables : int
            Total number of independent variables to be used in regression
        num_dep_variables : int
            Total number of dependent variables to be used in regression
        window_size : int, default 1
            window size of the rolling regression
        """
        super().__init__(num_ind_variables=num_ind_variables)
        self.number_of_dep_variables = num_dep_variables
        self.window_size = window_size

        self.wxx = window_base.WindowStream(
            window_shape=(
                window_size,
                num_ind_variables,
                num_ind_variables,
            ),
            curr_shape=(num_ind_variables, num_ind_variables),
            window_size=window_size,
        )

        self.wxy = window_base.WindowStream(
            curr_shape=(num_ind_variables, num_dep_variables),
            window_shape=(
                window_size,
                num_ind_variables,
                num_dep_variables,
            ),
            window_size=window_size,
        )

        self.wyy = window_base.WindowStream(
            curr_shape=(1, num_dep_variables),
            window_shape=(window_size, 1, num_dep_variables),
            window_size=window_size,
        )

        self._mask = window_base.WindowBase(
            window_shape=(window_size, 1, num_dep_variables),
            window_size=window_size,
        )

    @property
    def results(self) -> dict:
        if self.total_iterations < self.window_size:
            return self._results

        _mask_current = np.where(
            np.sum(self._mask.matrix, axis=0) == self.window_size, 1, np.nan
        )

        if self.wxx.matrix.shape[0] == 1:
            _S_wxx_inv = 1 / self.wxx.curr_vector
            self._results["beta"] = (_S_wxx_inv * self.wxy.curr_vector) * _mask_current
        else:
            _S_wxx_inv = np.linalg.pinv(self.wxx.curr_vector)
            self._results["beta"] = (_S_wxx_inv @ self.wxy.curr_vector) * _mask_current

        self._results["sigma"] = (
            (1 / (self.window_size - 1))
            * (
                self.wyy.curr_vector
                - np.sum(self.wxy.curr_vector * self._results["beta"], axis=0)
            )
        ).flatten().reshape(1, -1) * _mask_current

        self._results["tstat"] = (
            self._results["beta"]
            / np.sqrt(self._results["sigma"] * np.diag(_S_wxx_inv).reshape(-1, 1))
            * _mask_current
        )
        return self._results

    def update(
        self,
        batch_ind: np.ndarray,
        batch_dep: np.ndarray,
        batch_weight: float = 1,
    ) -> None:
        """
        Update rolling regression with new dependent and independent variable data

        Parameters
        ----------
        batch_ind : (1, number_ind_variables) numpy array
            The independent variables in this batch
        batch_dep : (number_of_dep_variables, 1) numpy array
            All of the dependant variables we want to regress on in this batch
        batch_weight : float, default 1
            The weight for this batch
        """
        self.total_iterations += 1

        _s_wyy_new = np.square(batch_dep) * batch_weight
        _s_wxy_new = batch_dep * batch_ind.T * batch_weight
        _S_wxx_new = batch_ind.T @ batch_ind * batch_weight
        _mask_new = np.where(np.isnan(batch_dep), 0, 1)

        self.wyy.update(new_vector=_s_wyy_new)
        self.wxy.update(new_vector=_s_wxy_new)
        self.wxx.update(new_vector=_S_wxx_new)
        self._mask.update(new_vector=_mask_new)
