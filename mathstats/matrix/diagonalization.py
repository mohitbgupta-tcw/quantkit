import scipy.cluster.hierarchy as sch
import numpy as np


def get_quasi_diag(matrix: np.ndarray) -> np.ndarray:
    """
    Reorganize rows and columns, so that the largest values lie along diagonal

    Parameters
    ----------
    matrix: np.array
        matrix

    Returns
    -------
    np.array
        array of order to sort columns in
    """
    return sch.to_tree(matrix, rd=False).pre_order()
