import quantkit.backtester.strategies.strategy as strategy
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import datetime


class PickAll(strategy.Strategy):
    """
    Base class that picks all stocks in the universe

    Parameters
    ----------
    params: dict
        strategy specific parameters which should include
            - type: "pick_all", str
            - window_size: lookback period in trading days, int
            - return_engine: str
            - risk_engine: str
            - allocation_models: weighting strategies, list
    """

    def __init__(self, params: dict) -> None:
        super().__init__(**params)

    @property
    def selected_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of selected securities

        Returns
        -------
        np.array
            array of indexes
        """
        ss = np.arange(self.num_total_assets)
        return ss[
            (
                ~np.isnan(self.risk_engine.cov_calculator.data_stream.matrix)
                .any(axis=0)
                .squeeze()
            )
            & (self.index_comp > 0)
        ]

    @property
    def return_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted DAILY returns from return engine in order of selected_securities

        Returns
        -------
        np.array
            returns
        """
        return self.return_metrics_intuitive[self.selected_securities]
