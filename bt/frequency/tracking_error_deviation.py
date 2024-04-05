import pandas as pd
import numpy as np
import sklearn.covariance
import quantkit.bt.core_structure.algo as algo


class PTE_Rebalance(algo.Algo):
    """
    Trigger rebalance when PTE (Predicted Tracking Error) from static weights is past specified level

    Parameters
    ----------
    PTE_volatility_cap: float
        annualized volatility deviation to target
    target_weights: pd.DataFrame
        DataFrame of weights - needs to have same index as price DataFrame
    lookback: DateOffset, optional
        lookback period for estimating volatility
    lag: DateOffset, optional
        amount of time to wait to calculate the covariance
    covar_method: str, optional
        method of calculating volatility
    annualization_factor: int, optional
        number of periods (days) to annualize by
    """

    def __init__(
        self,
        PTE_volatility_cap: float,
        target_weights: pd.DataFrame,
        lookback: pd.DateOffset = pd.DateOffset(months=3),
        lag: pd.DateOffset = pd.DateOffset(days=0),
        covar_method: str = "standard",
        annualization_factor: int = 252,
    ) -> None:
        super().__init__()
        self.PTE_volatility_cap = PTE_volatility_cap
        self.target_weights = target_weights
        self.lookback = lookback
        self.lag = lag
        self.covar_method = covar_method
        self.annualization_factor = annualization_factor

    def __call__(self, target) -> bool:
        """
        Run Algo on call PTE_Rebalance()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            needs rebalance
        """
        if "weights" in target.temp:
            current_weights = target.temp["weights"]
        else:
            return True

        target_weights = self.target_weights.loc[target.now, :].to_dict()

        cols = set(current_weights.keys()).copy()
        cols = list(cols.union(set(target_weights.keys())))

        weights = pd.Series(np.zeros(len(cols)), index=cols)
        for c in cols:
            if c in current_weights:
                weights[c] = current_weights[c]
            if c in target_weights:
                weights[c] -= target_weights[c]

        t0 = target.now - self.lag
        prc = target.universe.loc[t0 - self.lookback : t0, cols]
        returns = prc.pct_change().dropna()

        # calc covariance matrix
        if self.covar_method == "ledoit-wolf":
            covar = sklearn.covariance.ledoit_wolf(returns)
        elif self.covar_method == "standard":
            covar = returns.cov()
        else:
            raise NotImplementedError("covar_method not implemented")

        # print(target.now, cols)
        PTE_vol = np.sqrt(
            np.matmul(weights.values.T, np.matmul(covar.values, weights.values))
            * self.annualization_factor
        )

        if pd.isnull(PTE_vol):
            return False
        # vol is too high
        if PTE_vol > self.PTE_volatility_cap:
            return True
        else:
            return False
