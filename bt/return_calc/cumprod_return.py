import pandas as pd
import numpy as np
import bt.core_structure.algo as algo
from bt.util.logging import logging


class CumProdReturn(algo.Algo):
    """
    Calculate the cumprod return for each security in a given universe.
    -> save return values dataframe in temp['return']

    Parameters
    ----------
    lookback: DateOffset, optional
        lookback period in months
    lag: DateOffset, optional
        lag interval in days
    window_size: int, optional
        rolling window
    """

    def __init__(
        self,
        lookback: pd.DateOffset = pd.DateOffset(months=3),
        lag: pd.DateOffset = pd.DateOffset(days=0),
        window_size: int = None
    ) -> None:
        super().__init__()
        self.lookback = lookback
        self.lag = lag
        self.window_size = window_size

    def __call__(self, target) -> bool:
        """
        Run Algo on call CumProdReturn() and set temp['return'] with returns

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        logging.debug('Calculating cumprod returns')

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

        if 'return' in target.temp:
            logging.debug('Using predfined (algo) returns for cumprod calculation.')
            returns = target.temp['return']
        else:
            logging.debug('Using simple returns for cumprod calculation.')
            returns = prc.pct_change().dropna()

        if self.window_size is None:
            logging.debug('Calculating cumulative product of returns.')
            cumprod_return = ((returns+1).cumprod(skipna=True)-1).dropna()
        else:
            logging.debug('Calculating windowed cumulative product of returns. Window size = {}'.format(self.window_size))
            cumprod_return = ((returns+1).rolling(self.window_size).apply(np.prod)-1).dropna()

        target.temp['return'] = cumprod_return

        return True