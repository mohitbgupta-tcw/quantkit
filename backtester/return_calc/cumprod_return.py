import quantkit.backtester.return_calc.return_metrics as return_metrics
import quantkit.mathstats.sum.rolling_cumsum as rolling_cumsum
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import datetime


class CumProdReturn(return_metrics.ReturnMetrics):
    """
    Cumulative Return Calculation assuming
        - returns are log normal distributed
        - rolling historical window

    Parameters
    ----------
    universe: list
        investment universe
    frequency: str, optional
        frequency of index return data
    """

    def __init__(self, universe: list, frequency: str = None, **kwargs) -> None:
        super().__init__(universe)
        self.frequency = frequency
        self.return_calculator = rolling_cumsum.RollingCumSum(
            num_ind_variables=self.universe_size, **kwargs
        )

    @property
    def return_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_calculator.cumsum

    @property
    def return_metrics_intuitive(self) -> np.ndarray:
        """
        Forecaseted daily returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return annualize_adjustments.compound_annualization(
            self.return_calculator.cumsum, 1 / self.return_calculator.window_size
        )

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        annualize_factor=1.0,
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
        annualized_return = annualize_adjustments.compound_annualization(
            price_return, annualize_factor
        )

        annualized_return = np.squeeze(annualized_return)

        outgoing_row = np.squeeze(self.return_calculator.windowed_outgoing_row)
        self.return_calculator.update(
            (np.log(annualized_return + 1)), outgoing_row, index=date
        )

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.return_calculator.is_valid()
