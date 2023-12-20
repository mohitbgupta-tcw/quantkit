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
    """

    def __init__(
        self,
        universe: list,
        stop_threshold: float,
        frequency: str,
        rebalance: str,
        **kwargs,
    ) -> None:
        self.stop_threshold = np.log(1 - stop_threshold)
        self.rebalance = rebalance
        self.universe = universe
        self.num_total_assets = len(universe)
        self.frequency = frequency
        self.return_engine = cumprod_return.CumProdReturn(
            universe=universe,
            frequency=frequency,
            window_size=mapping_configs.trading_days[rebalance],
        )
        self.stopped_securities_matrix = list()
        self.prev_stopped = np.full(shape=self.num_total_assets, fill_value=False)

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
        Array with bool if security got stopped out

        Returns
        -------
        np.array
            array of indexes
        """
        raise NotImplementedError()

    def reset_engine(self) -> None:
        """
        Reset Stop Loss Engine
        """
        self.return_engine = cumprod_return.CumProdReturn(
            universe=self.universe,
            frequency=self.frequency,
            window_size=mapping_configs.trading_days[self.rebalance],
        )
        self.stopped_securities_matrix = list()
        self.prev_stopped = np.full(shape=self.num_total_assets, fill_value=False)
