import pandas as pd
import quantkit.bt.core_structure.algo as algo


class StatTotalReturn(algo.Algo):
    """
    Calculate total return over given period.
    -> save them in temp["stat"]

    Parameters
    ----------
    lookback: DateOffset
        lookback period in months
    lag: DateOffset
        lag interval in days
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
        Run Algo on call StatTotalReturn()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "selected" in target.temp:
            selected = target.temp["selected"]
        else:
            selected = list(target.universe.columns)
        t0 = target.now - self.lag
        if target.universe[selected].index[0] > t0:
            return False
        prc = target.universe.loc[t0 - self.lookback : t0, selected]
        tot_return = (prc.iloc[-1] / prc.iloc[0]) - 1
        target.temp["stat"] = tot_return
        return True
