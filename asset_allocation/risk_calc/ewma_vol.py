import quantkit.asset_allocation.risk_calc.log_vol as log_vol
import quantkit.mathstats.covariance.expo_covariance as expo_covariance
import quantkit.mathstats.time_series.decay as decay
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np


class LogNormalEWMA(log_vol.LogNormalVol):
    """Exponential Weighted Moving Average Covariance Calculation of Log Returns
    ** Assuming assets are log normal distribution
    """

    def __init__(self, universe, frequency=None, half_life=12, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run risk calculation on
        frequency: str, optional
            frequency of index return data
        half_life: int, optional
            length of time it takes to decrease to half of original amount
        """
        super().__init__(universe, frequency, **kwargs)
        self.cov_calculator = expo_covariance.ExponentialWeightedCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.cov_calculator_intuitive = expo_covariance.ExponentialWeightedCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.decay_factor = decay.decay_factor(half_life)

    def assign(self, date, price_return, annualize_factor=1.0):
        """
        Transform to log scale and assign returns to the actual calculator
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
        self.cov_calculator.update(
            np.log(annualized_return + 1), batch_weight=self.decay_factor, index=date
        )
        self.cov_calculator_intuitive.update(
            annualized_return, batch_weight=self.decay_factor, index=date
        )
        return


class RollingLogNormalEWMA(LogNormalEWMA):
    """Exponential Weighted Moving Average Covariance Calculation of Log Returns using Rolling Historical
    ** Assuming assets are log normal distribution
    """

    def __init__(self, universe, frequency=None, span=36, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run risk calculation on
        frequency: str, optional
            frequency of index return data
        span: int, optional
            number of data points used in calculation
        """
        super().__init__(universe, frequency, **kwargs)
        self.decay_factor = decay.decay_span(span)
