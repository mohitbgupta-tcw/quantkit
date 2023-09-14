import quantkit.mathstats.streaming_base.streaming_base as streaming_base
import quantkit.mathstats.streaming_base.window_base as window_base
import quantkit.mathstats.covariance.window_covariance as window_covariance
import quantkit.mathstats.matrix.correlation as correlation
import numpy as np


class WindowLS(streaming_base.StreamingBase):
    r"""
    A class for performing Ordinary Least Squared Regression (OLS) on windows of streaming data
    Calculates online version of sklearn.linear_model.LinearRegression()

    Model
    -----
    $$ dep_var = beta * ind_vars + mathcal{N}(0, sigma)$$

    Calculation
    -----------
    The standard least-square fit gives regression coefficients

    beta = (X'X)^{-1}X'Y
    (see https://stats.stackexchange.com/questions/6920/efficient-online-linear-regression/56642#56642)

    Parameters
    ----------
    num_ind_variables : int
        Total number of independent variables to be used in regression
    num_dep_variables : int
        Total number of dependent variables to be used in regression
    window_size : int, optional
        window size of the rolling regression
    """

    def __init__(
        self,
        num_ind_variables: int,
        num_dep_variables: int,
        window_size: int = 1,
    ) -> None:
        super().__init__(num_ind_variables=num_ind_variables)
        self.number_of_dep_variables = num_dep_variables
        self.window_size = window_size

        self.wxx = window_base.WindowStream(
            window_shape=(
                window_size,
                num_ind_variables + 1,
                num_ind_variables + 1,
            ),
            curr_shape=(num_ind_variables + 1, num_ind_variables + 1),
            window_size=window_size,
        )

        self.wxy = window_base.WindowStream(
            curr_shape=(num_ind_variables + 1, num_dep_variables),
            window_shape=(
                window_size,
                num_ind_variables + 1,
                num_dep_variables,
            ),
            window_size=window_size,
        )

        self._mask = window_base.WindowBase(
            window_shape=(window_size, 1, num_dep_variables),
            window_size=window_size,
        )

        self.cov_calculator = window_covariance.WindowCovariance(
            num_ind_variables=num_ind_variables + num_dep_variables,
            window_size=window_size,
        )

    @property
    def results(self) -> dict:
        r"""
        Generate a dictionary of results

        Note
        ----
        R squared can be calculated by

        r'_{y,x} * r_{x,x} * r_{y,x}

        See https://stats.stackexchange.com/questions/314926/can-you-calculate-r2-from-correlation-coefficents-in-multiple-linear-regressi/364630#364630

        Returns
        -------
        dict
            beta: slope of regression
            sigma: intercept of regression
            r_squared: r squared of regression
        """
        if self.total_iterations < self.window_size:
            return self._results

        _mask_current = np.where(
            np.sum(self._mask.matrix, axis=0) == self.window_size, 1, np.nan
        )

        if self.wxx.matrix.shape[0] == 1:
            _S_wxx_inv = 1 / self.wxx.curr_vector
            m = (_S_wxx_inv * self.wxy.curr_vector) * _mask_current
        else:
            _S_wxx_inv = np.linalg.pinv(self.wxx.curr_vector)
            m = (_S_wxx_inv @ self.wxy.curr_vector) * _mask_current

        _corr = correlation.cov_to_corr(self.cov_calculator.results["cov"])
        _ryx = _corr[: self.num_ind_variables, self.num_ind_variables :]
        _rxx = np.linalg.pinv(_corr[: self.num_ind_variables, : self.num_ind_variables])

        self._results["beta"] = m[1:]
        self._results["sigma"] = m[0]
        self._results["r_squared"] = np.diag(_ryx.T @ _rxx @ _ryx) * _mask_current
        return self._results

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
        _all_batch = np.append(batch_ind, batch_dep, 1).squeeze()
        self.cov_calculator.update(_all_batch)

        self.total_iterations += 1
        batch_ind = np.insert(batch_ind, 0, 1, axis=1)

        _s_wxy_new = batch_dep * batch_ind.T * batch_weight
        _S_wxx_new = batch_ind.T @ batch_ind * batch_weight
        _mask_new = np.where(np.isnan(batch_dep), 0, 1)

        self.wxy.update(new_vector=_s_wxy_new)
        self.wxx.update(new_vector=_S_wxx_new)
        self._mask.update(new_vector=_mask_new)
