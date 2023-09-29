import numpy as np
import quantkit.mathstats.streaming_base.weighted_base as weighted_base
import quantkit.utils.dataframe_utils as dataframe_utils


class SimpleCumProd(object):
    """
    Simple Cumulative Product Calculation
    Calculates the cumprod of an np.array
    Calculation in Incremental way:

        (previous product * incoming variables) / (outgoing variables)

    Parameters
    ----------
    num_ind_variables : int
        Number of variables
    """

    def __init__(self, num_ind_variables: int, **kwargs) -> None:
        self._cumprod = np.ones(shape=num_ind_variables) * np.nan
        self.data_stream = weighted_base.WeightedBase(
            matrix_shape=(1, num_ind_variables)
        )
        self.iterations = np.zeros(shape=num_ind_variables)
        self.total_iterations = 0

    @property
    def cumprod(self) -> np.array:
        """
        Returns
        -------
        np.array
            Cumulative Product of current array
        """
        return self._cumprod

    def calculate_cumprod(
        self,
        prev_prod: np.array,
        incoming_variables: np.array,
        outgoing_variables: np.array,
    ) -> np.array:
        """
        Calculate the cumulative product

        Calculation
        -----------
        (previous product * incoming variables) / (outgoing variables)

        Parameters
        ----------
        prev_prod: np.array
            previous product
        incoming_variables: np.array
            new set of values
        outgoing variables: np.array
            old set of values falling out of window size

        Returns
        -------
        np.array
            cumulative return
        """
        outgoing_variables = np.where(
            np.isnan(outgoing_variables), 1, outgoing_variables
        )

        new_cumprod = np.divide(
            np.apply_along_axis(
                dataframe_utils.nanprodwrapper, 0, [prev_prod, incoming_variables]
            ),
            outgoing_variables,
        )
        return new_cumprod

    def update(self, incoming_variables: np.ndarray, **kwargs) -> None:
        """
        Update the current cumprod array with newly streamed in data.

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
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
            new_vector=np.expand_dims(incoming_variables, axis=0),
            batch_weight=1,
            **kwargs,
        )
