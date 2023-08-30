import numpy as np
import quantkit.mathstats.streaming_base.weighted_base as weighted_base
import quantkit.utils.dataframe_utils as dataframe_utils


class SimpleCumProd(object):
    """
    Simple Cumulative Product Calculation
    Calculates the cumprod of an np.array

    Parameters
    ----------
    num_variables : int
        Number of variables
    """

    def __init__(self, num_variables: int, **kwargs):
        self._cumprod = np.ones(shape=num_variables) * np.nan
        self.data_stream = weighted_base.WeightedBase(matrix_shape=(1, num_variables))
        self.iterations = np.zeros(shape=num_variables)
        self.total_iterations = 0

    @property
    def cumprod(self) -> np.array:
        return self._cumprod

    def calculate_cumprod(
        self,
        prev_prod: np.array,
        incoming_variables: np.array,
        outgoing_variables: np.array,
    ) -> np.array:
        outgoing_variables = np.squeeze(
            np.nanmax([outgoing_variables, np.ones(outgoing_variables.shape)], axis=0)
        )

        new_cumprod = np.divide(
            np.apply_along_axis(
                dataframe_utils.nanprodwrapper, 0, [prev_prod, incoming_variables]
            ),
            outgoing_variables,
        )
        return new_cumprod

    def update(
        self, incoming_variables: np.ndarray, batch_weight: int = 1, **kwargs
    ) -> None:
        """
        Update the current mean array with newly streamed in data.

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        batch_weight : int, default 1
            Weight for the incoming stream of data
        """
        if self._cumprod.shape != incoming_variables.shape:
            raise RuntimeError(
                f"Incoming Variables shape {incoming_variables.shape} does not match Cumprod shape {self._cumprod.shape}"
            )

        self.total_iterations += 1
        self.iterations = self.iterations + np.where(np.isnan(incoming_variables), 0, 1)

        self._cumprod = self.calculate_cumprod(
            self._cumprod,
            incoming_variables,
            np.ones(shape=self._cumprod.shape) * np.nan,
        )

        self.data_stream.update(
            vector_one=np.expand_dims(incoming_variables, axis=0),
            batch_weight=batch_weight,
            **kwargs,
        )
