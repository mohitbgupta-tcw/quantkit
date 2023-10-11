import numpy as np
from collections import deque
from typing import Tuple


class WeightedBase(object):
    """
    Base class for working with weighted streams

    Parameters
    ----------
    matrix_shape : Tuple[int, ...]
        Tuple to specify matrix shape
    """

    def __init__(self, matrix_shape: Tuple[int, ...]) -> None:
        self._matrix = []
        self.current_vector = np.ones(matrix_shape) * np.nan
        self.current_loc = 0
        self._indexes = deque()

    @property
    def values(self) -> np.array:
        """
        Returns
        -------
        np.array
            array of current matrix
        """
        return np.array(self._matrix)

    @property
    def indexes(self) -> np.array:
        """
        Returns
        -------
        np.array
            array of all passed indexes
        """
        return np.array(self._indexes)

    def update(
        self,
        new_vector: np.array,
        batch_weight: float,
        adjustment: float = 1,
        index=None,
        **kwargs,
    ) -> None:
        """
        - Append new vector to matrix
        - Update current vector for cov calculation
        - Append index to indexes

        Parameters
        ----------
        new_vector : np.ndarray
            Input vector
        batch_weight : float
            Weight of incoming batch
        adjustment: float, optional
            adjust weight of old vectors over time
        index: optional
            index to save for streaming data point, p.e. date
        """
        self._matrix.append(new_vector)
        self.current_vector = np.nansum(
            [(self.current_vector * adjustment), (new_vector * batch_weight)], axis=0
        )

        this_index = index if index is not None else self.current_loc
        self._indexes.append(this_index)

        self.current_loc += 1
