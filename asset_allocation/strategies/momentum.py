import quantkit.asset_allocation.strategies.strategy as strategy
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
            - window_size: lookback period in trading days, int
            - return_engine: "cumprod", str
            - risk_engine: str
            - top_n: number of stocks to pick, int
            - allocation_models: weighting strategies, list
    """

    def __init__(self, params: dict) -> None:
        super().__init__(**params)
        self.window_size = params["window_size"]
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
        nan_sum = np.isnan(self.latest_return).sum()
        top_n = min(self.top_n, self.num_total_assets - nan_sum)
        neg_sort = (-self.return_metrics_intuitive).argsort()

        selected_assets = 0
        i = 0
        a = list()

        while selected_assets < top_n and i < self.num_total_assets:
            if self.index_comp[neg_sort[i]] > 0:
                a.append(neg_sort[i])
                selected_assets += 1
            i += 1
        return np.array(a)

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
