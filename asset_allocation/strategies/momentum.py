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

    def assign(
        self,
        date: datetime.date,
        price_return: np.array,
        annualize_factor: int = 1.0,
    ) -> None:
        """
        Transform and assign returns to the actual calculator

        Parameters
        ----------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency
        """
        self.return_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )
        self.portfolio_return_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )
        # only calculate cov mateix on rebalance dates to save time
        if date in self.rebalance_dates:
            self.risk_engine.assign(
                date=date, price_return=price_return, annualize_factor=annualize_factor
            )
            self.portfolio_risk_engine.assign(
                date=date, price_return=price_return, annualize_factor=annualize_factor
            )

    @property
    def selected_securities(self) -> np.array:
        """
        Index (position in universe_tickers as integer) of top n momentum securities

        Returns
        ------
        np.array
            array of indexes
        """
        return (-self.return_metrics_intuitive).argsort()[: self.top_n]

    @property
    def return_metrics_optimizer(self) -> np.array:
        """
        Forecaseted DAILY returns from return engine of top n momentum securities
        in order of selected_securities

        Parameter
        ---------

        Return
        ------
        np.array
            returns
        """
        returns_topn = self.return_metrics_intuitive[self.selected_securities]
        return annualize_adjustments.compound_annualization(
            returns_topn, 1 / self.window_size
        )
