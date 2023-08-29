import quantkit.asset_allocation.return_calc.return_metrics as return_metrics
import quantkit.mathstats.mean.simple_mean as simple_mean
import quantkit.mathstats.mean.rolling_mean as rolling_mean
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np


class SimpleExp(return_metrics.ReturnMetrics):
    """
    Simple Historical Return Calculation

    Parameters
    ----------
    factors: list
        factors to run return calculation on
    frequency: str, optional
        frequency of index return data
    """

    def __init__(self, universe, frequency=None, **kwargs):
        super().__init__(universe)
        self.frequency = frequency
        self.return_calculator = simple_mean.SimpleMean(
            num_variables=self.universe_size, **kwargs
        )
        self.return_calculator_window = rolling_mean.RollingMean(
            num_variables=self.universe_size, **kwargs
        )

    @property
    def return_metrics_optimizer(self):
        """
        Forecaseted returns from return engine

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        return self.return_calculator.gmean

    @property
    def return_metrics_intuitive(self):
        """
        Forecaseted returns from return engine

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        return self.return_metrics_optimizer

    @property
    def return_metrics_optimizer_window(self):
        """
        Forecaseted rolling historical return from return engine

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        return self.return_calculator_window.gmean

    @property
    def return_metrics_intuitive_window(self):
        """
        Forecaseted rolling historical return from return engine

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        return self.return_metrics_optimizer_window

    def get_portfolio_return(self, allocation, is_window=False, **kwargs):
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameter
        ---------
        allocation: np.array
            current allocation
        is_window: bool, optional
            calculate portfolio return based on simple or windowed returns

        Return
        ------
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
            allocation, this_returns, this_dates, **kwargs
        )

    def assign(
        self,
        date,
        price_return,
        annualize_factor=1.0,
    ):
        """
        Transform and assign returns to the actual calculator
        Parameter
        ---------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency

        Return
        ------
        """
        annualized_return = annualize_adjustments.compound_annualize(
            price_return, annualize_factor
        )
        self.return_calculator.update(annualized_return, index=date)

        outgoing_row = np.squeeze(self.return_calculator_window.windowed_outgoing_row)
        self.return_calculator_window.update(
            annualized_return, outgoing_row, index=date
        )
        return
