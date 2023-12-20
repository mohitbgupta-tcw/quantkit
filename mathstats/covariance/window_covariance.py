import quantkit.mathstats.streaming_base.streaming_base as streaming_base
import quantkit.mathstats.streaming_base.weighted_base as weighted_base
import quantkit.mathstats.mean.rolling_mean as rolling_mean
import numpy as np


class WindowCovariance(streaming_base.StreamingBase):
    """
    Rolling window covariance matrix calculation
    Implementation of pd.DataFrame.rolling().cov()

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

        self.rolling_mean = rolling_mean.RollingMean(
            num_ind_variables=num_ind_variables,
            window_size=window_size,
            ddof=0,
            **kwargs,
        )
        self._mean_sample = rolling_mean.RollingMean(
            num_ind_variables=num_ind_variables,
            window_size=window_size,
            ddof=ddof,
            **kwargs,
        )
        self._pair_mean = [
            rolling_mean.RollingMean(
                num_ind_variables=num_ind_variables,
                window_size=window_size,
                ddof=ddof,
                **kwargs,
            )
            for _i in range(num_ind_variables)
        ]

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
        mean_vec = np.expand_dims(np.array(self.rolling_mean.mean), axis=0)
        sample_mean_vec = np.expand_dims(np.array(self._mean_sample.mean), axis=0)
        pair_mean_matrix = np.stack(
            [self._pair_mean[i].mean for i in range(len(self._pair_mean))]
        )

        self._results["cov"] = np.asarray(
            pair_mean_matrix - sample_mean_vec.T @ mean_vec
        )
        self._results["variance"] = np.diagonal(self._results["cov"])

        self._results["mean"] = self.rolling_mean.mean
        self._results["gmean"] = self.rolling_mean.gmean
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

        outgoing_row_expanded = self.rolling_mean.windowed_outgoing_row
        outgoing_row = np.squeeze(outgoing_row_expanded, axis=0)
        batch_ind_expanded = np.expand_dims(batch_ind, axis=0)

        self._mean_sample.update(
            incoming_variables=batch_ind, outgoing_variables=outgoing_row, **kwargs
        )

        pair_product = batch_ind_expanded.T @ batch_ind_expanded
        outgoing_pair_product = outgoing_row_expanded.T @ outgoing_row_expanded
        for i, (x, y) in enumerate(zip(pair_product, outgoing_pair_product)):
            self._pair_mean[i].update(x, y, **kwargs)

        self.rolling_mean.update(
            incoming_variables=batch_ind, outgoing_variables=outgoing_row, **kwargs
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
