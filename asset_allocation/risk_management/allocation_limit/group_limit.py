import numpy as np
import pandas as pd
from typing import Union


def limit_group(
    weights: np.ndarray,
    universe: np.ndarray,
    limited_assets: np.ndarray,
    limit: float,
    max_allocation: np.ndarray,
    allocate_to: Union[np.ndarray, str],
) -> np.ndarray:
    """
    Reallocate weight from limited group to selected assets

    Parameters
    ----------
    weights: np.array
        weight allocation
    universe: np.array
        investment universe as integers
    limited_assets: np.arrays
        subset of universe which has weight upper bound
    limit: float
        upper bound for limited assets
    max_allocation: np.array
        maximum allocation per asset provided
    allocate_to: np.array | str
        where to allocate excess to
        set to "equal", if equal allocation to remaining assets
        set to array, if assigning to other assets
    """
    total_limited_assets = np.nansum(weights[limited_assets])
    excess = np.nanmax([total_limited_assets - limit, 0])
    weights[limited_assets] = weights[limited_assets] * (
        1 - excess / total_limited_assets
    )

    if isinstance(allocate_to, np.ndarray):
        for asset in allocate_to:
            allo = np.min([max_allocation[asset] - weights[asset], excess])
            weights[asset] += allo
            excess -= allo
    elif allocate_to == "equal":
        allocate_to_pos = np.where(~np.in1d(universe, limited_assets))[0]
        weights[allocate_to_pos] += excess / len(allocate_to_pos)
    return weights
