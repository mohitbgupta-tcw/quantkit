import pandas as pd
import numpy as np
import sklearn
from scipy.optimize import minimize
import quantkit.bt.core_structure.algo as algo


def calc_mean_var_weights(
    returns, weight_bounds=(0.0, 1.0), rf=0.0, covar_method="ledoit-wolf", options=None
):
    """
    Calculates the mean-variance weights given a DataFrame of returns.

    Args:
        * returns (DataFrame): Returns for multiple securities.
        * weight_bounds ((low, high)): Weigh limits for optimization.
        * rf (float): `Risk-free rate <https://www.investopedia.com/terms/r/risk-freerate.asp>`_ used in utility calculation
        * covar_method (str): Covariance matrix estimation method.
            Currently supported:
                - `ledoit-wolf <http://www.ledoit.net/honey.pdf>`_
                - standard
        * options (dict): options for minimizing, e.g. {'maxiter': 10000 }

    Returns:
        Series {col_name: weight}

    """

    def fitness(weights, exp_rets, covar, rf):
        # portfolio mean
        mean = sum(exp_rets * weights)
        # portfolio var
        var = np.dot(np.dot(weights, covar), weights)
        # utility - i.e. sharpe ratio
        util = (mean - rf) / np.sqrt(var)
        # negative because we want to maximize and optimizer
        # minimizes metric
        return -util

    n = len(returns.columns)

    # expected return defaults to mean return by default
    exp_rets = returns.mean()

    # calc covariance matrix
    if covar_method == "ledoit-wolf":
        covar = sklearn.covariance.ledoit_wolf(returns)[0]
    elif covar_method == "standard":
        covar = returns.cov()
    else:
        raise NotImplementedError("covar_method not implemented")

    weights = np.ones([n]) / n
    bounds = [weight_bounds for i in range(n)]
    # sum of weights must be equal to 1
    constraints = {"type": "eq", "fun": lambda W: sum(W) - 1.0}
    optimized = minimize(
        fitness,
        weights,
        (exp_rets, covar, rf),
        method="SLSQP",
        constraints=constraints,
        bounds=bounds,
        options=options,
    )
    # check if success
    if not optimized.success:
        raise Exception(optimized.message)

    # return weight vector
    return pd.Series({returns.columns[i]: optimized.x[i] for i in range(n)})


class MVOWeight(algo.Algo):
    """
    Mean-Variance Optimization implementation of Markowitz's model
    -> set temp["weights"] by calculating mvo weights

    Parameters
    ----------
    lookback: DateOffset, optional
        lookback period for estimating volatility
    bounds: tuple, optional
        tuple specifying min and max weights for each asset in optimization
    covar_method: str, optional
        method used to estimate the covariance
    rf: float, optional
        risk-free rate
    lag: DateOffset, optional
        amount of time to wait to calculate the covariance
    options: str, optional
        solver-specific options for convergence
    """

    def __init__(
        self,
        lookback: pd.DateOffset = pd.DateOffset(months=3),
        bounds=(0.0, 1.0),
        covar_method: str = "standard",
        rf: float = 0.0,
        lag: pd.DateOffset = pd.DateOffset(days=0),
        options: dict = None
    ) -> None:
        super().__init__()
        self.lookback = lookback
        self.lag = lag
        self.bounds = bounds
        self.covar_method = covar_method
        self.rf = rf
        self.options = options


    def __call__(self, target) -> bool:
        """
        Run Algo on call MVOWeight()

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
        tw = calc_mean_var_weights(
            prc.pct_change().dropna(),
            weight_bounds=self.bounds,
            covar_method=self.covar_method,
            rf=self.rf,
            options=self.options
        )

        target.temp["weights"] = tw.dropna().to_dict()
        return True
