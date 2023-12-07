import datetime
import numpy as np
import quantkit.asset_allocation.return_calc.cumprod_return as cumprod_return
import quantkit.utils.mapping_configs as mapping_configs


class StopLoss(object):
    """
    Base class to calculate Stop Loss Levels

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
        **kwargs,
    ) -> None:
        self.stop_threshold = np.log(1 - stop_threshold)
        self.rebalance = rebalance
        self.rebalance_dates = rebalance_dates
        self.universe = universe
        self.num_total_assets = len(universe)
        self.frequency = frequency
        self.return_engine = cumprod_return.CumProdReturn(
            universe=universe,
            frequency=frequency,
            window_size=mapping_configs.trading_days[rebalance],
        )

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
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
        annualize_factor: int, optional
            factor depending on data frequency
        """
        self.latest_return = price_return

        self.return_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )

    @property
    def stopped_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of all stopped out securities


        Returns
        -------
        np.array
            array of indexes
        """
        raise NotImplementedError()
