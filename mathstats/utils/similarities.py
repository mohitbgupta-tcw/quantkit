import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
from typing import Union


def cosine_similarity(matrix: Union[np.ndarray, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate cosine similarity based on matrix

    Parameters
    ----------
    matrix: np.array | pd.DataFrame
        matrix of vectors - if DataFrame, should have factors in columns and securities in index

    Returns
    -------
    pd.DataFrame
        cosine similarity
    """
    if isinstance(matrix, pd.DataFrame):
        cols = matrix.index
        matrix = matrix.to_numpy()
    else:
        cols = [i for i in range(matrix.shape[0])]

    cosine_sim = 1 - cdist(matrix, matrix, metric="cosine")
    cosine_sim = pd.DataFrame(cosine_sim, index=cols, columns=cols)
    return cosine_sim


def intersection(matrix: Union[np.ndarray, pd.DataFrame]) -> pd.DataFrame:
    r"""
    Calculate number of intersection between rows

    Parameters
    ----------
    matrix: np.array | pd.DataFrame
        matrix of vectors - if DataFrame, should have factors in columns and securities in index
        p.e.
        ticker	        AAPL US Equity	ACHR US Equity	AMD US Equity
        ARKQ US EQUITY	nan  	        1          	    1
        TGFTX US Equity	1       	    nan	            1
        CHAT US Equity	nan           	nan           	nan

    Returns
    -------
    pd.DataFrame:
        number of intersections
    """
    if isinstance(matrix, pd.DataFrame):
        cols = matrix.index
        matrix = matrix.to_numpy()
    else:
        cols = [i for i in range(matrix.shape[0])]

    final_m = np.zeros([matrix.shape[0], matrix.shape[0]])
    for col in matrix.T:
        sec_matrix = np.tile(col, (matrix.shape[0], 1)).T
        m = np.nan_to_num(np.minimum(sec_matrix, sec_matrix.T))
        final_m += m
    i = pd.DataFrame(final_m, index=cols, columns=cols)
    return i


def jaccard_similarity(matrix: Union[np.ndarray, pd.DataFrame]) -> pd.DataFrame:
    r"""
    Jaccard index is used to find similarity between two sample sets.
    It is defined as size (i.e., the number of elements) of the intersection between the two sets
    divided by the size of union of two sets.
    Jaccard index close to being 1 indicates high similarity, and close to 0 indicates little similarity.

    Calculation
    -----------
    $$
    J_{A,B} = \frac{|A\cap B|}{|A\cup B|}
    $$

    Parameters
    ----------
    matrix: np.array | pd.DataFrame
        matrix of vectors - if DataFrame, should have factors in columns and securities in index
        p.e.
        ticker	        AAPL US Equity	ACHR US Equity	AMD US Equity
        ARKQ US EQUITY	nan  	        1          	    1
        TGFTX US Equity	1       	    nan	            1
        CHAT US Equity	nan           	nan           	nan

    Returns
    -------
    pd.DataFrame:
        jaccard similarity
    """
    if isinstance(matrix, pd.DataFrame):
        cols = matrix.index
        matrix = matrix.to_numpy()
    else:
        cols = [i for i in range(matrix.shape[0])]

    i = intersection(matrix).to_numpy()
    jaccard_index = i / (
        np.tile(np.diag(i), (matrix.shape[0], 1))
        + np.tile(np.diag(i), (matrix.shape[0], 1)).T
        - i
    )
    jaccard_index = pd.DataFrame(jaccard_index, index=cols, columns=cols)
    return jaccard_index
