import numpy as np
import pandas as pd
from typing import Union
import quantkit.mathstats.sum.simple_cumsum as simple_cumsum
import quantkit.mathstats.streaming_base.window_base as window_base


class RollingCumSum(simple_cumsum.SimpleCumSum):
    """
    Rolling Cumulative Sum Calculation
    Calculates the rolling sum of an np.array
    Calculation in Incremental way:

        (previous sum + incoming variables) - (outgoing variables)


    Parameters
    ----------
    num_ind_variables : int
        Number of variables
    window_size: int
        lookback window
    """

    def __init__(
        self,
        num_ind_variables: int,
        window_size: int,
        **kwargs,
    ) -> None:
        self._cumsum = np.ones(shape=num_ind_variables) * np.nan
        self.total_iterations = 0
        self.iterations = np.zeros(shape=num_ind_variables)

        self.window_size = window_size

        self.data_stream = window_base.WindowBase(
            window_shape=(window_size, 1, num_ind_variables),
            window_size=window_size,
        )

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
        outgoing_variables: np.ndarray,
        **kwargs,
    ) -> None:
        """
        Update the current cumsum array with newly streamed in data.

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        outgoing_variables : np.array
            Outgoing stream of data
        """
        if self._cumsum.shape != incoming_variables.shape:
            raise RuntimeError(
                f"Incoming Variables shape {incoming_variables.shape} does not match Cumprod shape {self._cumsum.shape}"
            )

        self.total_iterations += 1

        self._cumsum = self.calculate_cumsum(
            self._cumsum,
            incoming_variables,
            outgoing_variables,
        )

        self.data_stream.update(
            np.expand_dims(incoming_variables, axis=0), incoming_variables, **kwargs
        )

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.total_iterations >= self.window_size
