import numpy as np
from copy import deepcopy
import quantkit.mathstats.mean.simple_mean as simple_mean


class ExponentialWeightedMean(simple_mean.SimpleMean):
    def __init__(self, num_variables: int, geo_base: int = 0, adjust=True, **kwargs):
        super().__init__(num_variables, geo_base, **kwargs)
        self.weight_sum = 0
        self._n_effective_ratio = 0
        self.adjust = adjust

    @property
    def mean(self) -> np.array:
        if self.adjust:
            return self._mean / np.max([self.weight_sum, 1])
        return self._mean

    @property
    def gmean(self) -> np.array:
        adjusted_gmean = self._gmean / self.weight_sum
        return (
            np.where(self._gmean == 0, np.nan, np.exp(adjusted_gmean)) - self.geo_base
        )

    @property
    def n_effective_ratio(self):
        return 1 - (self._n_effective_ratio / self.weight_sum**2)

    def calculate_adjusted(
        self,
        prev_average: np.array,
        incoming_variables: np.ndarray,
        batch_weight: int = 1,
        **kwargs,
    ):
        new_average = prev_average * batch_weight + incoming_variables
        return new_average

    def calculate_unadjusted(
        self,
        prev_average: np.array,
        incoming_variables: np.ndarray,
        batch_weight: int = 1,
        **kwargs,
    ):
        if self.total_iterations == 0:
            new_average = incoming_variables
            return new_average
        new_average = (
            1 - batch_weight
        ) * incoming_variables + batch_weight * prev_average
        return new_average

    def update(self, incoming_variables: np.ndarray, batch_weight: int = 1, **kwargs):
        """
        Update the current mean array with newly streamed in data.

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        batch_weight : int, default 1
            Weight for the incoming stream of data
        """
        if self._mean.shape != incoming_variables.shape:
            raise RuntimeError(
                f"Incoming Variables shape {incoming_variables.shape} does not match Mean shape {self._mean.shape}"
            )

        geo_incoming = incoming_variables + self.geo_base
        geo_incoming = np.log(np.where(geo_incoming <= 0, np.nan, geo_incoming))

        if self.adjust:
            self._mean = self.calculate_adjusted(
                prev_average=self._mean,
                incoming_variables=incoming_variables,
                batch_weight=batch_weight,
                **kwargs,
            )

        else:
            self._mean = self.calculate_unadjusted(
                prev_average=self._mean,
                incoming_variables=incoming_variables,
                batch_weight=batch_weight,
                **kwargs,
            )

        new_geometric_mean = self.calculate_adjusted(
            prev_average=self._gmean,
            incoming_variables=geo_incoming,
            batch_weight=batch_weight,
            **kwargs,
        )
        self._gmean = np.where(
            np.logical_or(np.isnan(geo_incoming), np.isnan(self._gmean)),
            np.nan,
            new_geometric_mean,
        )

        self.previous_weight_sum = deepcopy(self.weight_sum)
        self.weight_sum += batch_weight**self.total_iterations
        self._n_effective_ratio += batch_weight ** (2 * self.total_iterations)

        self.total_iterations += 1
        self.iterations = self.iterations + np.where(np.isnan(incoming_variables), 0, 1)

        self.data_stream.update(
            vector_one=np.expand_dims(incoming_variables, axis=0),
            batch_weight=batch_weight,
            **kwargs,
        )
        return
