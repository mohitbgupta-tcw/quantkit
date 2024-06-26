import numpy as np
import quantkit.mathstats.covariance.simple_covariance as simple_covariance
import quantkit.mathstats.mean.expo_weighted_mean as expo_weighted_mean


class ExponentialWeightedCovariance(simple_covariance.Covariance):
    """
    Exponential Weighted Covariance Matrix Calculation
    see https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf chapter 9

    Parameters
    ----------
    num_ind_variables : int
        Number of of independent variables
    min_observations : int, optional
        Number of observed data inputs before output is generated
    adjust: bool, optional
        calculate on adjusted or unadjusted version
    """

    def __init__(
        self, num_ind_variables: int, min_observations: int = 1, adjust=True, **kwargs
    ) -> None:
        super().__init__(
            num_ind_variables=num_ind_variables, min_observations=min_observations
        )
        self.adjust = adjust

        self.mean_calculator = expo_weighted_mean.ExponentialWeightedMean(
            num_ind_variables=num_ind_variables, adjust=adjust, **kwargs
        )

    def update_demeaned(
        self, vector_calc: np.ndarray, batch_weight: int = 1, **kwargs
    ) -> None:
        """
        Update covariance calculation
        See https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf chapter 9
        Formula 141

        Parameters
        ----------
        vector_calc : np.array
            vector of second summand of above calculation
        batch_weight: float
            Weight
        """
        if self.adjust:
            adjustment = (self.mean_calculator.weight_sum - 1) / np.maximum(
                self.mean_calculator.previous_weight_sum, 1
            )
            self.demean_squared.update(
                new_vector=vector_calc, batch_weight=1, adjustment=adjustment, **kwargs
            )
        else:
            self.demean_squared.update(
                new_vector=vector_calc,
                batch_weight=(1 - batch_weight),
                adjustment=batch_weight,
                **kwargs,
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

        adjustment = (
            np.maximum(self.mean_calculator.weight_sum, 1) if self.adjust else 1
        )

        self._results["cov"] = self.demean_squared.current_vector / adjustment

        self._results["mean"] = self.mean_calculator.mean
        self._results["gmean"] = self.mean_calculator.gmean
        return self._results
