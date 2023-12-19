import quantkit.asset_allocation.strategies.strategy as strategy
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

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        index_comp: np.ndarray,
        annualize_factor: int = 1.0,
        **kwargs,
    ) -> None:
        """
        Transform and assign returns to the actual calculator

        Parameters
        ----------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        index_comp: np.array
            index components for date
        annualize_factor: int, optional
            factor depending on data frequency
        """
        super().assign(date, price_return, index_comp, annualize_factor)

        self.return_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )
        self.portfolio_return_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )
        self.stop_loss.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )
        # only calculate cov matrix on rebalance dates to save time
        if date in self.rebalance_dates:
            self.risk_engine.assign(
                date=date, price_return=price_return, annualize_factor=annualize_factor
            )
            self.portfolio_risk_engine.assign(
                date=date, price_return=price_return, annualize_factor=annualize_factor
            )

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
        return ss[~np.isnan(self.latest_return) & self.index_comp]

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
