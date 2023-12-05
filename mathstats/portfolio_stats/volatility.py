import numpy as np


# Risk related statistics
def portfolio_vol(weights: np.ndarray, cov_matrix: np.ndarray) -> float:
    r"""
    Calculate portfolio volatility

    Calculation
    -----------
        sqrt(weights * covariance matrix * weights)

    Parameters
    ----------
    weights: np.array
        current weighting scheme of portfolio
    cov_matrix: np.array
        covariance matrix

    Returns
    -------
    float
        portfolio volatility
    """
    marginal_risk = marginal_risk_contribution(weights, cov_matrix)
    return np.sqrt(weights.T @ marginal_risk)


def marginal_risk_contribution(
    weights: np.ndarray, cov_matrix: np.ndarray
) -> np.ndarray:
    r"""
    Calculate marginal risk for each security in portfolio

    Calculation
    -----------
        covariance matrix * weights

    Parameters
    ----------
    weights: np.array
        current weighting scheme of portfolio
    cov_matrix: np.array
        covariance matrix

    Returns
    -------
    np.array
        marginal risk
    """
    return cov_matrix @ weights
