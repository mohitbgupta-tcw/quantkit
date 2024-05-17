import pandas as pd
import numpy as np
from quantkit.bt.util.logging import logging
import quantkit.bt.core_structure.algo as algo


def calc_inv_vol_weights(returns):
    """
    Calculates weights proportional to inverse volatility of each column.

    Returns weights that are inversely proportional to the column's
    volatility resulting in a set of portfolio weights where each position
    has the same level of volatility.

    Note, that assets with returns all equal to NaN or 0 are excluded from
    the portfolio (their weight is set to NaN).

    Returns:
        Series {col_name: weight}
    """
    # calc vols
    vol = np.divide(1.0, np.std(returns, ddof=1)).astype(float)
    vol[np.isinf(vol)] = np.NaN
    volsum = vol.sum()
    return np.divide(vol, volsum)


class InvVolWeight(algo.Algo):
    """
    Inverse Volatility Optimization implementation
    -> set temp["weights"] by calculating inv vol weights
    -> weights are proportional to the inverse of their volatility
    -> least volatile elements receive highest weight

    Parameters
    ----------
    lookback: DateOffset
        lookback period for estimating volatility
    lag: DateOffset, optional
        amount of time to wait to calculate the covariance
    """

    def __init__(self, lookback=pd.DateOffset(months=3), lag=pd.DateOffset(days=0)):
        super().__init__()
        self.lookback = lookback
        self.lag = lag

    def __call__(self, target) -> bool:
        """
        Run Algo on call InvVolWeight()

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
            logging.debug('Inverse Volatility optimization using returns algo')
            returns = target.temp['return']
        else:
            logging.debug('Inverse Volatility optimization using default simple returns')
            returns = prc.pct_change().dropna()

        tw = calc_inv_vol_weights(returns)
        target.temp["weights"] = tw.dropna().to_dict()
        return True
