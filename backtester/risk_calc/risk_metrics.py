import numpy as np
import datetime


class RiskMetrics(object):
    """
    Base class for building risk metrics

    Parameters
    ----------
    universe: list
        investment universe
    """

    def __init__(self, universe: list) -> None:
        self.universe = universe
        self.universe_size = len(universe)

    @property
    def risk_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        raise NotImplementedError

    @property
    def risk_metrics_intuitive(self) -> np.ndarray:
        """
        risk metrics for plotting needs to be human interpretable

        Returns
        -------
        np.array
            covariance matrix
        """
        raise NotImplementedError

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        annualize_factor: int = None,
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
        raise NotImplementedError

    @property
    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        raise NotImplementedError
