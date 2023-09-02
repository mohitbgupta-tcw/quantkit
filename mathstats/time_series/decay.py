import numpy as np


def decay_factor(half_life: int, time_periods: int = 1):
    """
    Given the half life of substance, return the decay factor for each time period

    Parameters
    ----------
    half_life: int
        half life in number of observations
    n_obs: int, optional
        number of time periods

    Return
    ------
    np.array
        array with decays for each time period
    """
    obs_range = np.arange(time_periods)[::-1] + 1
    return 0.5 ** (obs_range / half_life)


def decay_span(span: int) -> float:
    """
    Given the span of a time period, calculate the decay factor

    Parameters
    ----------
    span: int
        span length

    Returns
    -------
    float
        decay factor for span
    """
    return 2 / (span + 1)
