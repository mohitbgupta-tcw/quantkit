import numpy as np
import scipy.cluster.hierarchy as sch
import scipy.spatial.distance as ssd


def distance_matrix(matrix: np.ndarray) -> np.ndarray:
    r"""
    Calculate Distance Matrix of matrix

    Calculation
    -----------

        D = \sqrt{\frac{1}{2} (1-Cor(X)}

    Parameters
    ----------
    matrix: np.array
        matrix

    Returns
    -------
    np.array
        Distance matrix
    """
    return np.sqrt(np.clip((1.0 - matrix) / 2.0, a_min=0.0, a_max=1.0))


def linkage_matrix(distance_matrix: np.ndarray) -> np.ndarray:
    r"""
    Calculate Linkage Matrix of distance matrix

    Parameters
    ----------
    matrix: np.array
        distance matrix

    Returns
    -------
    np.array
        Linkage matrix
    """
    dist = ssd.squareform(distance_matrix, checks=False)
    return sch.linkage(dist, "single")
