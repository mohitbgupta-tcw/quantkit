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
