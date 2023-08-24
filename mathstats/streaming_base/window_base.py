import numpy as np
from collections import deque
from typing import Tuple


class WindowBase(object):
    """
    Base class for tracking matrices in the rolling window

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
    ):
        self.matrix = np.zeros(window_shape) * np.nan
        self.current_loc = 0
        self.window_size = window_size
        self._indexes = deque(maxlen=window_size)

    @property
    def values(self) -> np.array:
        ix_order = np.roll(np.arange(self.window_size), -self.current_loc)
        return self.matrix[ix_order]

    @property
    def indexes(self) -> np.array:
        return np.array(self._indexes)

    def update(
        self, vector_one: np.ndarray, batch_weight: int = 1, index=None, **kwargs
    ) -> None:
        """
        Update window matrix with new vector and increment location in
        window matrix

        Parameters
        ----------
        vector_one : np.ndarray
            Newly calculated vector from data streamed in
        index: optional
            index to save for streaming data point, p.e. date
        """
        self.matrix[self.current_loc, :, :] = vector_one
        self.current_loc = (self.current_loc + 1) % self.window_size

        if index is not None:
            self._indexes.append(index)
