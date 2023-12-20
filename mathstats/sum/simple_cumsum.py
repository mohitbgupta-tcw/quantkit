import numpy as np
import quantkit.mathstats.streaming_base.weighted_base as weighted_base


class SimpleCumSum(object):
    """
    Simple Cumulative Sum Calculation
    Calculates the sum of an np.array
    Calculation in Incremental way:

        (previous sum + incoming variables) - (outgoing variables)

    Parameters
    ----------
    num_ind_variables : int
        Number of variables
    """

    def __init__(self, num_ind_variables: int, **kwargs) -> None:
        self._cumsum = np.ones(shape=num_ind_variables) * np.nan
        self.data_stream = weighted_base.WeightedBase(
            matrix_shape=(1, num_ind_variables)
        )
        self.iterations = np.zeros(shape=num_ind_variables)
        self.total_iterations = 0

    @property
    def cumsum(self) -> np.ndarray:
        """
        Returns
        -------
        np.array
            Cumulative Sum of current array
        """
        return self._cumsum

    def calculate_cumsum(
        self,
        prev_sum: np.ndarray,
        incoming_variables: np.ndarray,
        outgoing_variables: np.ndarray,
    ) -> np.ndarray:
        """
        Calculate the cumulative sum

        Calculation
        -----------
        (previous sum + incoming variables) - (outgoing variables)

        Parameters
        ----------
        prev_sum: np.array
            previous sum
        incoming_variables: np.array
            new set of values
        outgoing variables: np.array
            old set of values falling out of window size

        Returns
        -------
        np.array
            cumulative sum
        """
        outgoing_variables = np.where(
            np.isnan(outgoing_variables), 0, outgoing_variables
        )

        new_cumsum = np.nansum(
            [prev_sum, incoming_variables, -outgoing_variables], axis=0
        )
        return new_cumsum

    def update(self, incoming_variables: np.ndarray, **kwargs) -> None:
        """
        Update the current cumsum array with newly streamed in data.

        Parameters
        ----------
        incoming_variables : np.array
            Incoming stream of data
        """
        if self._cumsum.shape != incoming_variables.shape:
            raise RuntimeError(
                f"Incoming Variables shape {incoming_variables.shape} does not match Cumsum shape {self._cumsum.shape}"
            )

        self.total_iterations += 1
        self.iterations = self.iterations + np.where(np.isnan(incoming_variables), 0, 1)

        self._cumsum = self.calculate_cumsum(
            self._cumsum,
            incoming_variables,
            np.ones(shape=self._cumsum.shape) * np.nan,
        )

        self.data_stream.update(
            new_vector=np.expand_dims(incoming_variables, axis=0),
            batch_weight=1,
            **kwargs,
        )

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.total_iterations > 0
