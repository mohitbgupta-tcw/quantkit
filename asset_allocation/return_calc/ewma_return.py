import quantkit.asset_allocation.return_calc.log_return as log_return
import quantkit.mathstats.mean.expo_weighted_mean as expo_weighted_mean
import quantkit.utils.annualize_adjustments as annualize_adjustments
import quantkit.mathstats.time_series.decay as decay
import numpy as np
import datetime


class LogEWMA(log_return.LogReturn):
    """
    Exponential Weighted Moving Average Calculation assuming:
        - returns are log normal distributed

    Parameters
    ----------
    universe: list
        investment universe
    frequency: str, optional
        frequency of index return data
    half_life: int, optional
        length of time it takes to decrease to half of original amount
    """

    def __init__(
        self, universe: list, frequency: str = None, half_life: int = 12, **kwargs
    ) -> None:
        super().__init__(universe, frequency, **kwargs)
        self.return_calculator = expo_weighted_mean.ExponentialWeightedMean(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.decay_factor = decay.decay_factor(half_life)[0]

    def assign(
        self,
        date: datetime.date,
        price_return: np.array,
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

        self.return_calculator.update(
            np.log(annualized_return + 1),
            batch_weight=self.decay_factor,
            index=date,
        )


class RollingLogEWMA(LogEWMA):
    """
    Rolling Exponential Weighted Moving Average assuming
        - returns are log normal distributed
        - rolling historical window

    Parameters
    ----------
    universe: list
        investment universe
    frequency: str, optional
        frequency of index return data
    span: int, optional
        number of data points used in calculation
    """

    def __init__(
        self, universe: list, frequency: str = None, span: int = 36, **kwargs
    ) -> None:
        super().__init__(universe, frequency, **kwargs)
        self.decay_factor = decay.decay_span(span)
