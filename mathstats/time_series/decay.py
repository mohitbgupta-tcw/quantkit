import numpy as np


def decay_factor(half_life, n_obs=None):
    """Given half life of substance, return decay factor

    Parameter
    ---------
    n_obs: int
        number of observations
    half_life: int
        half life in number of observations

    Return
    ------
    decay_factor: float or ArrayLike
    """
    obs_range = np.arange(n_obs)[::-1] + 1 if n_obs is not None else 1
    return 0.5 ** (obs_range / half_life)


def decay_span(span):
    return 2 / (span + 1)
