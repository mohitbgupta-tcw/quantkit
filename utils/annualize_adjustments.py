import numpy as np


def simple_annualize(return_series, annualize_factor):
    """
    Simple multiplication

    Parameter
    ---------
    return_series: np.array or float
        zero base return series or number
    annualize_factor: int
        factor depending on frequency

    Return
    ------
    np.array
        zero base annualized return series or number
    """
    if annualize_factor == 1.0:
        return return_series

    return return_series * annualize_factor


def compound_annualize(return_series, annualize_factor):
    """
    Compound Annualization

    Parameter
    ---------
    return_series: np.array or float
        zero base return series or number
    annualize_factor: int
        factor depending on frequency

    Return
    ------
    np.array
        zero base annualized return series or number
    """
    if annualize_factor == 1.0:
        return return_series

    return (return_series + 1) ** annualize_factor - 1


def volatility_annualize(volatility_series, annualize_factor):
    """
    Volatility Annualization
    """
    if annualize_factor == 1.0:
        return volatility_series

    return volatility_series * np.sqrt(annualize_factor)


def volatility_de_annualize(volatility_series, annualize_factor):
    """
    Convert volatility to monthly
    """
    if annualize_factor == 1.0:
        return volatility_series

    return volatility_series / np.sqrt(annualize_factor)


def covariance_annualize(cov_matrix, annualize_factor):
    """
    Covariance Annualization
    """
    if annualize_factor == 1.0:
        return cov_matrix

    return cov_matrix * annualize_factor
