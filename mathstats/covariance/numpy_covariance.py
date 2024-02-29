import numpy as np
import quantkit.mathstats.streaming_base.streaming_base as streaming_base
import quantkit.mathstats.streaming_base.window_base as window_base


class NumpyWindowCovariance(streaming_base.StreamingBase):
    """
    Rolling window covariance matrix calculation

    Parameters
    ----------
    num_ind_variables : int
        Number of independent variables
    window_size : int
        Size of the rolling window
    ddof: int
        degrees of freedom
    """

    def __init__(
        self, num_ind_variables: int, window_size: int, ddof: int = 0, **kwargs
    ) -> None:
        super().__init__(num_ind_variables=num_ind_variables)
        self.window_size = window_size
        self.ddof = ddof

        self.data_stream = window_base.WindowBase(
            window_shape=(window_size, 1, num_ind_variables),
            window_size=window_size,
        )

    @property
    def results(self) -> dict:
        """
        Generate a dictionary of results containing covariance matrix and mean

        Returns
        -------
        dict
            cov: covariance matrix
            mean: mean
            gmean: gmean
        """
        self._results["cov"] = np.cov(
            self.data_stream.matrix.squeeze(), rowvar=0, ddof=self.ddof
        )
        self._results["variance"] = np.diagonal(self._results["cov"])

        self._results["mean"] = np.mean(self.data_stream.matrix.squeeze(), axis=0)
        return self._results

    def update(self, batch_ind: np.ndarray, **kwargs) -> None:
        """
        Updates the window covariance matrix with new streamed data

        Parameters
        ----------
        batch_ind : np.array
            Independent variable data
        """
        self.total_iterations += 1

        self.data_stream.update(np.expand_dims(batch_ind, axis=0), **kwargs)

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.total_iterations >= self.window_size
