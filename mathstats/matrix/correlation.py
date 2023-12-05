import pandas as pd
import numpy as np
from typing import Union


def cov_to_corr(covariance: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
    """
    Converting covariance matrix to correlation matrix

    Parameters
    ----------
    covariance: np.array | pd.DataFrame
        covariance matrix

    Returns
    -------
    np.array
        correlation matrix
    """
    variances = np.sqrt(np.diag(covariance))
    outer_variances = np.outer(variances, variances)
    correlation = covariance / outer_variances
    correlation[covariance == 0.0] = 0.0
    return correlation


def corr_to_cov(
    correlation: Union[np.ndarray, pd.DataFrame],
    standard_deviations: Union[np.ndarray, pd.DataFrame],
) -> np.ndarray:
    """
    Converting correlation matrix and new vector of standard deviations to covariance matrix

    Parameters
    ----------
    correlation: np.array | pd.DataFrame
        correlation matrix
    standard_deviations: np.array | pd.DataFrame
        standard deviations

    Returns
    -------
    np.array
        covariance matrix
    """
    outer_variances = np.outer(standard_deviations, standard_deviations)
    covariance = correlation * outer_variances
    covariance[correlation == 0.0] = 0.0
    return covariance
