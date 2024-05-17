import pandas as pd
import numpy as np
import bt.core_structure.algo as algo
from bt.util.logging import logging


class LogReturn(algo.Algo):
    """
    Calculate the log return for each security in a given universe.
    -> save return values dataframe in temp['return']

    Parameters
    ----------
    lookback: DateOffset
        lookback period in months. Note that if a subsequent weighting algo
        has a lookback set, then this should be the same.
    lag: DateOffset
        lag interval in days. Note that if a subsequent weighting algo
        has a lag set, then this should be the same.
    """

    def __init__(
        self,
        lookback: pd.DateOffset = pd.DateOffset(months=3),
        lag: pd.DateOffset = pd.DateOffset(days=0),
    ) -> None:
        super().__init__()
        self.lookback = lookback
        self.lag = lag

    def __call__(self, target) -> bool:
        """
        Run Algo on call LogReturn() and set temp['return'] with returns

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        logging.debug('Calculating log returns')

        if 'selected' in target.temp:
            selected = target.temp['selected']
        else:
            selected = target.universe.columns

        t0 = target.now - self.lag

        if target.universe[selected].index[0] > t0:
            return False
        
        prc = target.universe.loc[t0 - self.lookback : t0, selected]

        if len(prc.dropna()) < 2:
            return False

        log_return =  np.log(prc/prc.shift(1)).dropna()
        target.temp['return'] = log_return

        return True