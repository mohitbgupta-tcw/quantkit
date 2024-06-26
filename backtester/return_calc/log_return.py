import quantkit.backtester.return_calc.return_metrics as return_metrics
import quantkit.mathstats.mean.rolling_mean as rolling_mean
import quantkit.mathstats.mean.numpy_mean as numpy_mean
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import pandas as pd
import datetime


class LogReturn(return_metrics.ReturnMetrics):
    """
    Return Calculation assuming
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
        self.return_calculator = numpy_mean.NumpyWindowMean(
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
        return self.return_calculator.mean

    @property
    def return_metrics_intuitive(self) -> np.ndarray:
        """
        Forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_metrics_optimizer

    def get_portfolio_return(self, allocation: np.ndarray, **kwargs) -> pd.DataFrame:
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameters
        ----------
        allocation: np.array
            current allocation

        Returns
        -------
        pd.DataFrame
            return: float

        """
        this_returns = np.exp(self.return_calculator.data_stream.values)
        this_dates = self.return_calculator.data_stream.indexes
        return super().get_portfolio_return(
            allocation=allocation,
            this_returns=this_returns,
            indexes=this_dates,
            **kwargs,
        )

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
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
        annualized_return = annualize_adjustments.compound_annualization(
            price_return, annualize_factor
        )

        annualized_return = np.squeeze(annualized_return)

        outgoing_row = np.squeeze(self.return_calculator.windowed_outgoing_row)
        self.return_calculator.update(
            incoming_variables=np.log(annualized_return + 1),
            outgoing_variables=outgoing_row,
            index=date,
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
