import pandas as pd
import numpy as np
import sklearn
from quantkit.bt.util.logging import logging
from scipy.optimize import minimize
import quantkit.bt.core_structure.algo as algo


def _erc_weights_ccd(x0, cov, b, maximum_iterations, tolerance):
    """
    Calculates the equal risk contribution / risk parity weights given
    a DataFrame of returns.

    Args:
        * x0 (np.array): Starting asset weights.
        * cov (np.array): covariance matrix.
        * b (np.array): Risk target weights.
        * maximum_iterations (int): Maximum iterations in iterative solutions.
        * tolerance (float): Tolerance level in iterative solutions.

    Returns:
        np.array {weight}

    Reference:
        Griveau-Billion, Theophile and Richard, Jean-Charles and Roncalli,
        Thierry, A Fast Algorithm for Computing High-Dimensional Risk Parity
        Portfolios (2013).
        Available at SSRN: https://ssrn.com/abstract=2325255

    """
    n = len(x0)
    x = x0.copy()
    var = np.diagonal(cov)
    ctr = cov.dot(x)
    sigma_x = np.sqrt(x.T.dot(ctr))

    for iteration in range(maximum_iterations):
        for i in range(n):
            alpha = var[i]
            beta = ctr[i] - x[i] * alpha
            gamma = -b[i] * sigma_x

            x_tilde = (-beta + np.sqrt(beta * beta - 4 * alpha * gamma)) / (2 * alpha)
            x_i = x[i]

            ctr = ctr - cov[i] * x_i + cov[i] * x_tilde
            sigma_x = sigma_x * sigma_x - 2 * x_i * cov[i].dot(x) + x_i * x_i * var[i]
            x[i] = x_tilde
            sigma_x = np.sqrt(
                sigma_x + 2 * x_tilde * cov[i].dot(x) - x_tilde * x_tilde * var[i]
            )

        # check convergence
        if np.power((x - x0) / x.sum(), 2).sum() < tolerance:
            return x / x.sum()

        x0 = x.copy()

    # no solution found
    raise ValueError(
        "No solution found after {0} iterations.".format(maximum_iterations)
    )


def _erc_weights_slsqp(x0, cov, b, maximum_iterations, tolerance):
    """
    Calculates the equal risk contribution / risk parity weights given
        a DataFrame of returns.

    Args:
    * x0 (np.array): Starting asset weights.
    * cov (np.array): covariance matrix.
    * b (np.array): Risk target weights. By definition target total risk contributions are all equal which makes this redundant.
    * maximum_iterations (int): Maximum iterations in iterative solutions.
    * tolerance (float): Tolerance level in iterative solutions.

    Returns:
    np.array {weight}

    You can read more about ERC at
    http://thierry-roncalli.com/download/erc.pdf

    """

    def fitness(weights, covar):
        # total risk contributions
        # trc = weights*np.matmul(covar,weights)/np.sqrt(np.matmul(weights.T,np.matmul(covar,weights)))

        # instead of using the true definition for trc we will use the optimization on page 5
        trc = weights * np.matmul(covar, weights)

        n = len(trc)
        # sum of squared differences of total risk contributions
        sse = 0.0
        for i in range(n):
            for j in range(n):
                # switched from squared deviations to absolute deviations to avoid numerical instability
                sse += np.abs(trc[i] - trc[j])
        # minimizes metric
        return sse

    # nonnegative
    bounds = [(0, None) for i in range(len(x0))]
    # sum of weights must be equal to 1
    constraints = {"type": "eq", "fun": lambda W: sum(W) - 1.0}
    options = {"maxiter": maximum_iterations}

    optimized = minimize(
        fitness,
        x0,
        (cov),
        method="SLSQP",
        constraints=constraints,
        bounds=bounds,
        options=options,
        tol=tolerance,
    )
    # check if success
    if not optimized.success:
        raise Exception(optimized.message)

    # return weight vector
    return optimized.x


def calc_erc_weights(
    returns,
    initial_weights=None,
    risk_weights=None,
    covar_method="ledoit-wolf",
    risk_parity_method="ccd",
    maximum_iterations=100,
    tolerance=1e-8,
):
    """
    Calculates the equal risk contribution / risk parity weights given a
    DataFrame of returns.

    Args:
        * returns (DataFrame): Returns for multiple securities.
        * initial_weights (list): Starting asset weights [default inverse vol].
        * risk_weights (list): Risk target weights [default equal weight].
        * covar_method (str): Covariance matrix estimation method.
            Currently supported:
                - `ledoit-wolf <http://www.ledoit.net/honey.pdf>`_ [default]
                - standard
        * risk_parity_method (str): Risk parity estimation method.
            Currently supported:
                - ccd (cyclical coordinate descent)[default]
                - slsqp (scipy's implementation of sequential least squares programming)
        * maximum_iterations (int): Maximum iterations in iterative solutions.
        * tolerance (float): Tolerance level in iterative solutions.

    Returns:
        Series {col_name: weight}

    """
    n = len(returns.columns)

    # calc covariance matrix
    if covar_method == "ledoit-wolf":
        covar = sklearn.covariance.ledoit_wolf(returns)[0]
    elif covar_method == "standard":
        covar = returns.cov().values
    else:
        raise NotImplementedError("covar_method not implemented")

    # initial weights (default to inverse vol)
    if initial_weights is None:
        inv_vol = 1.0 / np.sqrt(np.diagonal(covar))
        initial_weights = inv_vol / inv_vol.sum()

    # default to equal risk weight
    if risk_weights is None:
        risk_weights = np.ones(n) / n

    # calc risk parity weights matrix
    if risk_parity_method == "ccd":
        # cyclical coordinate descent implementation
        erc_weights = _erc_weights_ccd(
            initial_weights, covar, risk_weights, maximum_iterations, tolerance
        )
    elif risk_parity_method == "slsqp":
        # scipys slsqp optimizer
        erc_weights = _erc_weights_slsqp(
            initial_weights, covar, risk_weights, maximum_iterations, tolerance
        )

    else:
        raise NotImplementedError("risk_parity_method not implemented")

    # return erc weights vector
    return pd.Series(erc_weights, index=returns.columns, name="erc")


class RiskParityWeight(algo.Algo):
    """
    Risk-Parity Optimization implementation
    -> set temp["weights"] by calculating risk parity weights

    Parameters
    ----------
    lookback: DateOffset, optional
        lookback period for estimating covariance
    initial_weights: list, optional
        Starting asset weights
    risk_weights: list , optional
        Risk target weights
    covar_method: str, optional
        method used to estimate covariance
    risk_parity_method: str, optional
        risk parity estimation method
    maximum_iterations: int, optional
        maximum iterations in iterative solutions
    tolerance: float, optional
        tolerance level in iterative solutions
    lag: DateOffset, optional
        amount of time to wait to calculate the covariance
    """

    def __init__(
        self,
        lookback: pd.DateOffset = pd.DateOffset(months=3),
        initial_weights: list = None,
        risk_weights: list = None,
        covar_method: str = "standard",
        risk_parity_method: str = "ccd",
        maximum_iterations: int = 100,
        tolerance: float = 1e-8,
        lag: pd.DateOffset = pd.DateOffset(days=0),
    ) -> None:
        super().__init__()
        self.lookback = lookback
        self.initial_weights = initial_weights
        self.risk_weights = risk_weights
        self.covar_method = covar_method
        self.risk_parity_method = risk_parity_method
        self.maximum_iterations = maximum_iterations
        self.tolerance = tolerance
        self.lag = lag

    def __call__(self, target) -> bool:
        """
        Run Algo on call RiskParityWeight()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        selected = target.temp["selected"]

        if len(selected) == 0:
            target.temp["weights"] = {}
            return True

        if len(selected) == 1:
            target.temp["weights"] = {selected[0]: 1.0}
            return True

        t0 = target.now - self.lag
        prc = target.universe.loc[t0 - self.lookback : t0, selected]

        if 'return'in target.temp:
            logging.debug('Risk Parity optimization using returns algo')
            returns = target.temp['return']
        else:
            logging.debug('Risk Parity optimization using default simple returns')
            returns = prc.pct_change().dropna()

        tw = calc_erc_weights(
            returns,
            initial_weights=self.initial_weights,
            risk_weights=self.risk_weights,
            covar_method=self.covar_method,
            risk_parity_method=self.risk_parity_method,
            maximum_iterations=self.maximum_iterations,
            tolerance=self.tolerance,
        )

        target.temp["weights"] = tw.dropna().to_dict()
        return True
