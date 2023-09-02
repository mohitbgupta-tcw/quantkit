import numpy as np
import pandas as pd
from typing import Union
import quantkit.mathstats.product.simple_cumprod as simple_cumprod
import quantkit.mathstats.streaming_base.window_base as window_base


class RollingCumProd(simple_cumprod.SimpleCumProd):
    """
    Rolling Cumulative Product Calculation
    Calculates the rolling cumprod of an np.array

    Parameters
    ----------
    num_variables : int
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
        num_variables: int,
        window_size: int,
        **kwargs,
    ) -> None:
        self._cumprod = np.ones(shape=num_variables) * np.nan
        self.total_iterations = 0
        self.iterations = np.zeros(shape=num_variables)

        self.window_size = window_size

        self.data_stream = window_base.WindowBase(
            window_shape=(window_size, 1, num_variables),
            curr_shape=(1, num_variables),
            window_size=window_size,
        )

    @property
    def windowed_outgoing_row(self) -> np.array:
        return self.data_stream.matrix[self.data_stream.current_loc, :, :]

    def update(
        self,
        incoming_variables: Union[np.ndarray, pd.Series],
        outgoing_variables: np.array,
        **kwargs,
    ) -> None:
        """
        Update the current mean array with newly streamed in data.
        New Mean = Previous Mean + (Incoming Data - Outgoing Data) / N number of variables

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        outgoing_variables : np.array, default None
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
