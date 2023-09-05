import quantkit.mathstats.streaming_base.streaming_base as streaming_base
import quantkit.mathstats.streaming_base.weighted_base as weighted_base
import quantkit.mathstats.mean.simple_mean as simple_mean
import numpy as np


class Covariance(streaming_base.StreamingBase):
    """
    Covariance Matrix Calculation
    Implementation of pd.DataFrame.cov()

    Calculation in Incremental way:

        previous covariance + (incoming variables - previous average) * (incoming variables - average)

    see https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf chapter 3

    Parameters
    ----------
    num_ind_variables : int
        Number of of independent variables
    min_observations : int, optional
        Number of observed data inputs before output is generated
    """

    def __init__(
        self, num_ind_variables: int, min_observations: int = 1, **kwargs
    ) -> None:
        super().__init__(num_ind_variables=num_ind_variables)
        self.min_observations = min_observations

        self.mean_calculator = simple_mean.SimpleMean(
            num_ind_variables=num_ind_variables, **kwargs
        )
        self.demean_squared = weighted_base.WeightedBase(
            matrix_shape=(num_ind_variables, num_ind_variables)
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
        if self.total_iterations < self.min_observations:
            return

        self._results["cov"] = self.demean_squared.current_vector / np.minimum(
            (self.mean_calculator.iterations - 1),
            (self.mean_calculator.iterations - 1).T,
        )

        self._results["mean"] = self.mean_calculator.mean
        self._results["gmean"] = self.mean_calculator.gmean
        return self._results

    def update_demeaned(
        self, vector_calc: np.array, batch_weight: int = 1, **kwargs
    ) -> None:
        """
        Update covariance calculation
        See https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf chapter 3
        Formula 24

        Calculation
        -----------
        previous covariance + (incoming variables - previous average) * (incoming variables - average)

        Parameters
        ----------
        vector_calc : np.array
            vector of second summand of above calculation
        batch_weight: float
            Weight
        """
        self.demean_squared.update(
            new_vector=vector_calc, batch_weight=batch_weight, **kwargs
        )

    def update(self, batch_ind: np.array, batch_weight: float = 1, **kwargs) -> None:
        """
        Update the covariance matrix with new data streamed in

        Parameters
        ----------
        batch_ind : np.array
            Independent variable data
        batch_weight: float
            Weight
        """
        self.total_iterations += 1
        previous_mean = np.expand_dims(self.mean_calculator.mean, axis=0)
        self.mean_calculator.update(
            incoming_variables=batch_ind, batch_weight=batch_weight, **kwargs
        )

        this_mean = np.expand_dims(self.mean_calculator.mean, axis=0)
        batch_ind_array = np.expand_dims(batch_ind, axis=0)
        vector_calc = (batch_ind_array - this_mean).T @ (
            batch_ind_array - previous_mean
        )

        self.update_demeaned(
            vector_calc=vector_calc, batch_weight=batch_weight, **kwargs
        )
