import quantkit.asset_allocation.return_calc.log_return as log_return
import quantkit.mathstats.mean.expo_weighted_mean as expo_weighted_mean
import quantkit.utils.annualize_adjustments as annualize_adjustments
import quantkit.mathstats.time_series.decay as decay
import numpy as np


class LogEWMA(log_return.LogReturn):
    """Exponential Weighted Moving Average Calculation of Log Returns
    ** Assuming assets are log normal distribution
    """

    def __init__(self, universe, frequency=None, half_life=12, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run return calculation on
        frequency: str, optional
            frequency of index return data
        half_life: int, optional
            length of time it takes to decrease to half of original amount
        """
        super().__init__(universe, frequency, **kwargs)
        self.return_calculator = expo_weighted_mean.ExponentialWeightedMean(
            num_variables=self.universe_size, **kwargs
        )
        self.decay_factor = decay.decay_factor(half_life)[0]

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
        annualized_return = np.squeeze(annualized_return)

        self.return_calculator.update(
            np.log(annualized_return + 1),
            batch_weight=self.decay_factor,
            index=date,
        )
        return


class RollingLogEWMA(LogEWMA):
    """Exponential Weighted Moving Average of Log Returns using Rolling Historical
    ** Assuming assets are log normal distribution
    """

    def __init__(self, factors, frequency=None, span=36, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run return calculation on
        frequency: str, optional
            frequency of index return data
        span: int, optional
            number of data points used in calculation
        """
        super().__init__(factors, frequency, **kwargs)
        self.decay_factor = decay.decay_span(span)
