import numpy as np
import pandas as pd
from typing import Union
import quantkit.mathstats.product.simple_cumprod as simple_cumprod
import quantkit.mathstats.streaming_base.window_base as window_base


class RollingCumProd(simple_cumprod.SimpleCumProd):
    """
    Rolling Cumulative Product Calculation
    Calculates the rolling cumprod of an np.array
    Calculation in Incremental way:

        (previous product * incoming variables) / (outgoing variables)


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
        self._cumprod = np.ones(shape=num_ind_variables) * np.nan
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
        Update the current cumprod array with newly streamed in data.

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        outgoing_variables : np.array
            Outgoing stream of data
        """
        if self._cumprod.shape != incoming_variables.shape:
            raise RuntimeError(
                f"Incoming Variables shape {incoming_variables.shape} does not match Cumprod shape {self._cumprod.shape}"
            )

        self.total_iterations += 1

        self._cumprod = self.calculate_cumprod(
            self._cumprod,
            incoming_variables,
            outgoing_variables,
        )

        self.data_stream.update(
            np.expand_dims(incoming_variables, axis=0), incoming_variables, **kwargs
        )
