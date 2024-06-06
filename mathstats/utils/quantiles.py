import numpy as np
import pandas as pd


def quantile_df(df: pd.DataFrame, breakpoints: list) -> pd.DataFrame:
    """
    Calculate quantiles per column

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with numerical columns
    breakpoints: list
        breakpoints for quantiles, list of decimals, p.e.
        [0.1, 0.2, 0.4, 0.6, 0.8, 0.9] for quintiles with 10% outlier cutoff

    Returns
    -------
    pd.DataFrame:
        DataFrame with Quantiles in index
    """
    quintile_df = pd.DataFrame(
        np.nanquantile(df.to_numpy(), breakpoints, axis=0),
        columns=df.columns,
        index=[f"quantile {i}" for i in range(len(breakpoints))],
    )
    return quintile_df
