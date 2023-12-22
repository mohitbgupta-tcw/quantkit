import datetime
import numpy as np
import quantkit.asset_allocation.risk_management.stop_loss.stop_loss as stop_loss
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
    """

    def __init__(
        self,
        universe: list,
        stop_threshold: float,
        frequency: str,
        rebalance: str,
        **kwargs,
    ) -> None:
        super().__init__(universe, stop_threshold, frequency, rebalance, **kwargs)

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        annualize_factor: int = 1,
        **kwargs,
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
        super().assign(date, price_return, annualize_factor, **kwargs)
        self.stopped_securities_matrix.append(self.stopped_securities)

    @property
    def stopped_securities(self) -> np.ndarray:
        """
        Array with bool if security got stopped out

        Returns
        -------
        np.array
            array of indexes
        """
        stopped = np.logical_or(
            self.return_engine.return_metrics_optimizer < self.stop_threshold,
            self.prev_stopped,
        )
        self.prev_stopped = stopped
        return stopped
