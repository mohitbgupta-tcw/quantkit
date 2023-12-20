import numpy as np
import pandas as pd
from typing import Union
import quantkit.mathstats.mean.simple_mean as simple_mean
import quantkit.mathstats.streaming_base.window_base as window_base


class RollingMean(simple_mean.SimpleMean):
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
    def gmean(self) -> np.ndarray:
        """
        Returns
        -------
        np.array
            geometric mean of current array
        """
        if self.total_iterations < self.window_size:
            return np.zeros(self._gmean.shape) * np.nan
        return (
            np.squeeze(
                np.where(
                    self.data_stream.is_positive(adjustment=self.geo_base),
                    np.exp(self._gmean),
                    np.nan,
                ),
                axis=0,
            )
            - self.geo_base
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
        Update the current mean array with newly streamed in data

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        outgoing_variables : np.array
            Outgoing stream of data
        """
        if self._mean.shape != incoming_variables.shape:
            raise RuntimeError(
                f"Incoming Variables shape {incoming_variables.shape} does not match Mean shape {self._mean.shape}"
            )

        self.total_iterations += 1

        self._mean = self.calculate_average(
            prev_average=self._mean,
            incoming_variables=incoming_variables,
            outgoing_variables=outgoing_variables,
            num_variables=self.window_size,
        )

        geo_incoming = incoming_variables + self.geo_base
        geo_incoming = np.log(np.where(geo_incoming <= 0, np.nan, geo_incoming))

        geo_outgoing = outgoing_variables + self.geo_base
        geo_outgoing = np.log(np.where(geo_outgoing <= 0, np.nan, geo_outgoing))

        self._gmean = self.calculate_average(
            prev_average=self._gmean,
            incoming_variables=geo_incoming,
            outgoing_variables=geo_outgoing,
            num_variables=self.window_size,
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
