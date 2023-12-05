import numpy as np


def inverse_variance(matrix: np.ndarray) -> float:
    """
    Calculate inverse variance of matrix

    Parameters
    ----------
    matrix: np.array
        symmetric matrix

    Returns
    -------
    float
        inverse variance
    """
    weights = 1 / np.diag(matrix)
    weights /= weights.sum()
    return np.linalg.multi_dot((weights, matrix, weights))


def sliced_inverse_variance(matrix: np.ndarray, subset: list) -> float:
    """
    Calculate inverse variance of slice matrix

    Parameters
    ----------
    matrix: np.array
        symmetric matrix
    subset: list
        slice of matrix

    Returns
    -------
    float
        inverse variance
    """
    matrix = matrix[np.ix_(subset, subset)]
    return inverse_variance(matrix)
