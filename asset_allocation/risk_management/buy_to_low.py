import datetime
import numpy as np
import quantkit.asset_allocation.risk_management.stop_loss as stop_loss
import quantkit.asset_allocation.return_calc.cumprod_return as cumprod_return
import quantkit.utils.mapping_configs as mapping_configs


class BuyToLow(stop_loss.StopLoss):
    """
    Stop out Security if it falls more than x% since Rebalance Date

    Parameters
    ----------
    universe: list
        investment universe
    stop_threshold: float
        stop threshold percentage
    frequency: str
        frequency of return data
    rebelance: str
        rebalance frequency
    rebalance_dates: list
        list of rebalancing dates
    """

    def __init__(
        self,
        universe: list,
        stop_threshold: float,
        frequency: str,
        rebalance: str,
        rebalance_dates: list,
        **kwargs
    ) -> None:
        super().__init__(
            universe, stop_threshold, frequency, rebalance, rebalance_dates, **kwargs
        )

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        annualize_factor: int = 1,
        **kwargs
    ) -> None:
        """
        Transform and assign returns to the actual calculator
        On rebalance date, reset return engine

        Parameters
        ----------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency
        """
        if date in self.rebalance_dates:
            self.return_engine = cumprod_return.CumProdReturn(
                universe=self.universe,
                frequency=self.frequency,
                window_size=mapping_configs.trading_days[self.rebalance],
            )

        return super().assign(date, price_return, annualize_factor, **kwargs)

    @property
    def stopped_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of all stopped out securities


        Returns
        -------
        np.array
            array of indexes
        """
        ss = np.arange(self.num_total_assets)
        return ss[self.return_engine.return_metrics_optimizer < self.stop_threshold]
