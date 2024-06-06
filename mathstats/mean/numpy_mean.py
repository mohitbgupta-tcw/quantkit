import numpy as np
import pandas as pd
from typing import Union
import quantkit.mathstats.mean.simple_mean as simple_mean
import quantkit.mathstats.streaming_base.window_base as window_base


class NumpyWindowMean(object):
    """
    Rolling Mean Calculation
    Calculates the rolling mean of an np.array
    Calculation in Incremental way:

        previous average + (incoming variables - outgoing variables) / number of variables

    Parameters
    ----------
    num_ind_variables : int
        Number of variables
    window_size: int
        lookback window
    ddof: int, optional
        degrees of freedom
    geo_base: int, optional
        geo base for geometric mean calculation
    """

    def __init__(
        self,
        num_ind_variables: int,
        window_size: int,
        ddof: int = None,
        geo_base: int = 0,
        **kwargs,
    ) -> None:
        self.geo_base = geo_base
        self._mean = np.zeros(shape=num_ind_variables)
        self._gmean = np.ones(shape=num_ind_variables) * np.nan
        self.total_iterations = 0

        self.window_size = window_size if ddof is None else (window_size - ddof)

        self.data_stream = window_base.WindowBase(
            window_shape=(window_size, 1, num_ind_variables),
            window_size=window_size,
        )

    @property
    def mean(self) -> np.ndarray:
        """
        Returns
        -------
        np.array
            mean of current array
        """
        if self.window_size == 1:
            return self.data_stream.matrix.squeeze()
        return np.mean(self.data_stream.matrix.squeeze(), axis=0)

    @property
    def windowed_outgoing_row(self) -> np.ndarray:
        """
        Return the outgoing row (FIFO - first in first out)
        of the rolling window

        Returns
        -------
        np.array
            outgoing row
        """
        return self.data_stream.matrix[self.data_stream.current_loc, :, :]

    def update(
        self,
        incoming_variables: Union[np.ndarray, pd.Series],
        **kwargs,
    ) -> None:
        """
        Update the current mean array with newly streamed in data

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        """
        self.total_iterations += 1

        self.data_stream.update(np.expand_dims(incoming_variables, axis=0), **kwargs)

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.total_iterations >= self.window_size
