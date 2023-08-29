import quantkit.asset_allocation.return_calc.return_metrics as return_metrics
import quantkit.mathstats.mean.rolling_mean as rolling_mean
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np


class LogReturn(return_metrics.ReturnMetrics):
    """Return Calculation Assuming Log Normal Distribution using Rolling Historical"""

    def __init__(self, universe, frequency=None, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run return calculation on
        frequency: str, optional
            frequency of index return data
        """
        super().__init__(universe)
        self.frequency = frequency
        self.return_calculator = rolling_mean.RollingMean(
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
        return self.return_calculator.mean

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

    def get_portfolio_return(self, allocation, **kwargs):
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameter
        ---------
        allocation: np.array
            current allocation

        Return
        ------
        pd.DataFrame
            return: float

        """
        this_returns = np.exp(self.return_calculator.data_stream.values)
        this_dates = self.return_calculator.data_stream.indexes
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

        outgoing_row = np.squeeze(self.return_calculator.windowed_outgoing_row)
        self.return_calculator.update(
            np.log(annualized_return + 1), outgoing_row, index=date
        )
        return
