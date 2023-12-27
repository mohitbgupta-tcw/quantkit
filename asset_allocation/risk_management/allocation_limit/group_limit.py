import numpy as np
import pandas as pd
from typing import Union


def limit_group(
    weights: np.ndarray,
    universe: list,
    limited_assets: list,
    limit: float,
    max_allocation: list,
    allocate_to: Union[list, str],
) -> np.ndarray:
    """
    Reallocate weight from limited group to selected assets

    Parameters
    ----------
    weights: np.array
        weight allocation
    universe: list
        investment universe
    limited_assets: list
        subset of universe which has weight upper bound
    limit: float
        upper bound for limited assets
    max_allocation: np.array
        maximum allocation per asset provided
    allocate_to: list | str
        where to allocate list to
        set to "equal", if equal allocation to remaining assets
        set to ordered list, if assigning to other assets
    """
    limited_pos = [universe.index(i) for i in limited_assets]
    allocate_to_pos = [universe.index(i) for i in universe if i not in limited_assets]
    total_limited_assets = np.nansum(weights[limited_pos])
    excess = np.nanmax([total_limited_assets - limit, 0])
    weights[limited_pos] = weights[limited_pos] * (1 - excess / total_limited_assets)

    if isinstance(allocate_to, list):
        for asset in [universe.index(i) for i in allocate_to]:
            allo = np.min([max_allocation[asset] - weights[asset], excess])
            weights[asset] += allo
            excess -= allo
    elif allocate_to == "equal":
        weights[allocate_to_pos] += excess / len(allocate_to_pos)
    return weights
