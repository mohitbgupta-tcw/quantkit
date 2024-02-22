import quantkit.backtester.return_calc.return_metrics as return_metrics
import quantkit.mathstats.mean.simple_mean as simple_mean
import quantkit.mathstats.mean.rolling_mean as rolling_mean
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import pandas as pd
import datetime


class SimpleExp(return_metrics.ReturnMetrics):
    """
    Simple Historical Return Calculation

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
        self.return_calculator = simple_mean.SimpleMean(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.return_calculator_window = rolling_mean.RollingMean(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.kwargs = kwargs

    @property
    def return_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_calculator.gmean

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

    @property
    def return_metrics_optimizer_window(self) -> np.ndarray:
        """
        Forecaseted rolling historical return from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_calculator_window.gmean

    @property
    def return_metrics_intuitive_window(self) -> None:
        """
        Forecaseted rolling historical return from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_metrics_optimizer_window

    def get_portfolio_return(
        self, allocation: np.ndarray, is_window: bool = False, **kwargs
    ) -> pd.DataFrame:
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameters
        ----------
        allocation: np.array
            current allocation
        is_window: bool, optional
            calculate portfolio return based on simple or windowed returns

        Returns
        -------
        pd.DataFrame
            return: float
        """
        if is_window:
            this_returns = self.return_calculator_window.data_stream.values
            this_dates = self.return_calculator_window.data_stream.indexes
        else:
            this_returns = self.return_calculator.data_stream.values
            this_dates = self.return_calculator.data_stream.indexes

        # This is necessary due to the structure of streaming module
        this_returns = this_returns[:, 0, :]

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
        self.return_calculator.update(annualized_return, index=date)

        outgoing_row = np.squeeze(self.return_calculator_window.windowed_outgoing_row)
        self.return_calculator_window.update(
            annualized_return, outgoing_row, index=date
        )

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.return_calculator_window.is_valid()

    def reset_engine(self) -> None:
        """
        Reset Return Engine
        """
        self.return_calculator = simple_mean.SimpleMean(
            num_ind_variables=self.universe_size, **self.kwargs
        )
        self.return_calculator_window = rolling_mean.RollingMean(
            num_ind_variables=self.universe_size, **self.kwargs
        )
