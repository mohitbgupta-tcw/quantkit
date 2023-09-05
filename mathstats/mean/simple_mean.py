from typing import Union
import numpy as np
import quantkit.mathstats.streaming_base.weighted_base as weighted_base


class SimpleMean(object):
    """
    Simple Mean Calculation
    Calculates the mean of an np.array
    Calculation in Incremental way:

        previous average + (incoming variables - previous average) / number of variables

    see https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf chapter 1

    Parameters
    ----------
    num_ind_variables : int
        Number of variables
    geo_base: int, optional
        geo base for geometric mean calculation
    """

    def __init__(self, num_ind_variables: int, geo_base: int = 0, **kwargs) -> None:
        self.geo_base = geo_base
        self._mean = np.zeros(shape=num_ind_variables)
        self._gmean = np.zeros(shape=num_ind_variables)

        self.data_stream = weighted_base.WeightedBase(
            matrix_shape=(1, num_ind_variables)
        )
        self.iterations = np.zeros(shape=num_ind_variables)
        self.total_iterations = 0

    @property
    def mean(self) -> np.array:
        """
        Returns
        -------
        np.array
            mean of current array
        """
        return self._mean

    @property
    def gmean(self) -> np.array:
        """
        Returns
        -------
        np.array
            geometric mean of current array
        """
        return np.where(self._gmean == 0, np.nan, np.exp(self._gmean)) - self.geo_base

    def calculate_average(
        self,
        prev_average: np.array,
        incoming_variables: np.array,
        outgoing_variables: np.array,
        num_variables: Union[np.array, int],
    ) -> np.array:
        """
        Calculate average

        Calculation
        -----------
        previous average + (incoming variables - outgoing variables) / number of variables

        Parameters
        ----------
        prev_average : np.array
            Previous Average
        incoming_variables : np.array
            Incoming variables
        outgoing_variables : np.array
            Outgoing variables
        num_variables : np.array | int
            number of variables

        Returns
        -------
        np.array
            Newly calculated average
        """

        new_average = np.nansum(
            [
                prev_average,
                np.nansum([incoming_variables, -outgoing_variables], axis=0)
                / num_variables,
            ],
            axis=0,
        )

        return new_average

    def update(
        self, incoming_variables: np.ndarray, batch_weight: int = 1, **kwargs
    ) -> None:
        """
        Update the current mean array with newly streamed in data.

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        batch_weight : int, optional
            Weight for the incoming stream of data
        """
        if self._mean.shape != incoming_variables.shape:
            raise RuntimeError(
                f"Incoming Variables shape {incoming_variables.shape} does not match Mean shape {self._mean.shape}"
            )

        self.total_iterations += 1
        self.iterations = self.iterations + np.where(np.isnan(incoming_variables), 0, 1)

        self._mean = self.calculate_average(
            prev_average=self._mean,
            incoming_variables=incoming_variables,
            outgoing_variables=self._mean,
            num_variables=self.iterations,
        )

        geo_incoming = incoming_variables + self.geo_base
        geo_incoming = np.log(np.where(geo_incoming <= 0, np.nan, geo_incoming))

        new_geometric_mean = self.calculate_average(
            prev_average=self._gmean,
            incoming_variables=geo_incoming,
            outgoing_variables=self._gmean,
            num_variables=self.iterations,
        )

        self._gmean = np.where(
            np.logical_or(np.isnan(geo_incoming), np.isnan(self._gmean)),
            np.nan,
            new_geometric_mean,
        )

        self.data_stream.update(
            new_vector=np.expand_dims(incoming_variables, axis=0),
            batch_weight=batch_weight,
            **kwargs,
        )
