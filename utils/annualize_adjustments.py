import numpy as np
import pandas as pd
from typing import Union


def simple_annualization(
    return_series: Union[pd.Series, np.array, float], annualization_factor: int
) -> np.array:
    """
    Approximated annualization of returns by simple multiplication of return series with annualization factor

    Calculation
    -----------
    return series * annualization factor

    Parameters
    ----------
    return_series: pd.Series | np.array | float
        zero based returns
    annualize_factor: int
        annualization factor depending on frequency

    Returns
    -------
    np.array
        zero based annualized returns
    """
    return return_series * annualization_factor


def compound_annualization(
    return_series: Union[pd.Series, np.array, float], annualization_factor: int
) -> np.array:
    """
    Exact annualization of returns by compounding

    Calculation
    -----------
    ((return series + 1) ** annualization factor) - 1

    Parameters
    ----------
    return_series: pd.Series | np.array | float
        zero based returns
    annualize_factor: int
        annualization factor depending on frequency

    Returns
    -------
    np.array
        zero base annualized return series or number
    """
    return (return_series + 1) ** annualization_factor - 1


def volatility_annualize(
    volatility_series: Union[pd.Series, np.array, float], annualization_factor: int
) -> np.array:
    """
    Exact annualization of volatility by compounding

    Calculation
    -----------
    volatility series * sqrt(annualization factor)

    Parameters
    ----------
    volatility_series: pd.Series | np.array | float
        volatility
    annualize_factor: int
        annualization factor depending on frequency

    Returns
    -------
    np.array
        annualized volatility
    """
    return volatility_series * np.sqrt(annualization_factor)


def volatility_de_annualize(
    volatility_series: Union[pd.Series, np.array, float], annualization_factor: int
) -> np.array:
    """
    Exact de-annualization of volatility by compounding,
    i.e. from annual returns to monthly returns

    Calculation
    -----------
    volatility series / sqrt(annualization factor)

    Parameters
    ----------
    volatility_series: pd.Series | np.array | float
        volatility
    annualize_factor: int
        annualization factor depending on frequency

    Returns
    -------
    np.array
        de-annualized volatility
    """
    return volatility_series / np.sqrt(annualization_factor)


def covariance_annualize(
    cov_matrix: Union[pd.DataFrame, np.array], annualization_factor: int
) -> np.array:
    """
    Exact annualization of covariance matrix by compounding

    Calculation
    -----------
    cov matrix * annualization factor

    Parameters
    ----------
    cov_matrix: pd.DataFrame | np.array
        covariance matrix
    annualize_factor: int
        annualization factor depending on frequency

    Returns
    -------
    np.array
        annualized covariance matrix
    """
    return cov_matrix * annualization_factor
