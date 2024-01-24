import quantkit.asset_allocation.strategies.strategy as strategy
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import datetime


class MeanReversion(strategy.Strategy):
    """
    "Strong prices in the short term should reverse"

    Base class for Simple Momentum Strategy
    Idea: Take top n securities based on cumulative returns in window_size

    Parameters
    ----------
    params: dict
        strategy specific parameters which should include
            - type: "mean_reversion", str
            - return_window_size: lookback period in trading days, int
            - return_engine: "cumprod", str
            - risk_engine: str
            - decile: decile to pick, int
            - allocation_models: weighting strategies, list
    """

    def __init__(self, params: dict) -> None:
        super().__init__(**params)
        self.window_size = params["return_window_size"]
        self.fraud_threshold = params["fraud_threshold"]
        self.decile = params["decile"]

    @property
    def selected_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of top mean reversion securities

        Returns
        -------
        np.array
            array of indexes
        """
        (tradeable,) = np.where(
            (self.index_comp > 0)
            & (~np.isnan(self.latest_return))
            & (self.return_engine.return_metrics_optimizer > -self.fraud_threshold)
        )
        neg_sort = tradeable[np.argsort(self.return_metrics_intuitive[tradeable])]

        lower_bound = round(len(tradeable) / 10 * (self.decile - 1))
        upper_bound = round(len(tradeable) / 10 * self.decile)
        return neg_sort[lower_bound:upper_bound]

    @property
    def return_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted DAILY returns from return engine of top reversion securities
        in order of selected_securities

        Returns
        -------
        np.array
            returns
        """
        return self.return_metrics_intuitive[self.selected_securities]
