import quantkit.backtester.strategies.strategy as strategy
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import datetime


class Momentum(strategy.Strategy):
    """
    "Buy Low, Sell High."

    Base class for Simple Momentum Strategy
    Idea: Take top n securities based on cumulative returns in window_size

    Parameters
    ----------
    params: dict
        strategy specific parameters which should include
            - type: "momentum", str
            - return_window_size: lookback period in trading days, int
            - return_engine: "cumprod", str
            - risk_engine: str
            - top_n: number of stocks to pick, int
            - allocation_models: weighting strategies, list
    """

    def __init__(self, params: dict) -> None:
        super().__init__(**params)
        self.window_size = params["return_window_size"]
        self.top_n = params["top_n"]

    @property
    def selected_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of top n momentum securities

        Returns
        -------
        np.array
            array of indexes
        """
        (tradeable,) = np.where((self.index_comp > 0) & (~np.isnan(self.latest_return)))
        neg_sort = tradeable[np.argsort(-self.return_metrics_intuitive[tradeable])]
        return neg_sort[: self.top_n]

    @property
    def return_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted DAILY returns from return engine of top n momentum securities
        in order of selected_securities

        Returns
        -------
        np.array
            returns
        """
        return self.return_metrics_intuitive[self.selected_securities]
