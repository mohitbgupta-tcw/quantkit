import numpy as np
from collections import deque
from typing import Tuple


class WindowBase(object):
    """
    Base class for tracking matrix values in the rolling window

    Parameters
    ----------
    window_shape : Tuple[int, ...]
        Shape of the window matrix
    window_size : int, optional
        Size of the rolling window
    """

    def __init__(
        self,
        window_shape: Tuple[int, ...],
        window_size: int = 1,
    ) -> None:
        self.matrix = np.ones(window_shape) * np.nan
        self.current_loc = 0
        self.window_size = window_size
        self._indexes = deque(maxlen=window_size)

    @property
    def values(self) -> np.array:
        """
        Returns
        -------
        np.array
            array of current matrix
        """
        ix_order = np.roll(np.arange(self.window_size), -self.current_loc)
        return self.matrix[ix_order]

    @property
    def indexes(self) -> np.array:
        """
        Returns
        -------
        np.array
            array of current indexes
        """
        return np.array(self._indexes)

    def is_positive(self, adjustment=0) -> bool:
        """
        Returns
        -------
        bool
            all values in matrix are positive
        """
        return np.all(self.matrix + adjustment > 0, axis=0)

    def update(
        self, new_vector: np.array, batch_weight: float = 1, index=None, **kwargs
    ) -> None:
        """
        - Update window matrix with new vector
        - Update location for window matrix

        Parameters
        ----------
        new_vector : np.array
            Input vector
        batch_weight : float, optional
            Weight of incoming batch
        index: optional
            index to save for streaming data point, p.e. date
        """
        self.matrix[self.current_loc, :, :] = new_vector
        self.current_loc = (self.current_loc + 1) % self.window_size

        if index is not None:
            self._indexes.append(index)
