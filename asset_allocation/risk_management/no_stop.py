import datetime
import numpy as np
import quantkit.asset_allocation.risk_management.stop_loss as stop_loss


class NoStop(stop_loss.StopLoss):
    """
    No Stop Strategy

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
        super().__init__(
            universe, stop_threshold, frequency, rebalance, rebalance_dates, **kwargs
        )

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
        return np.full(shape=self.num_total_assets, fill_value=False)
