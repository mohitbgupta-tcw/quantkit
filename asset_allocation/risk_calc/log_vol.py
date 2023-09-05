import quantkit.asset_allocation.risk_calc.risk_metrics as risk_metrics
import quantkit.mathstats.covariance.window_covariance as window_covariance
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np


class LogNormalVol(risk_metrics.RiskMetrics):
    """Simple Covariance Calculation of Log Return using Rolling Historical
    ** Assuming assets are log normal distribution
    """

    def __init__(self, universe, frequency=None, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run risk calculation on
        frequency: str, optional
            frequency of index return data
        """
        super().__init__(universe)
        self.frequency = frequency
        self.cov_calculator = window_covariance.WindowCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.cov_calculator_intuitive = window_covariance.WindowCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )

    @property
    def risk_metrics_optimizer(self):
        """
        Forecaseted log normal historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov

    @property
    def cov(self):
        """
        Forecaseted log normal historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov_calculator.results["cov"]

    @property
    def risk_metrics_intuitive(self):
        """
        For descriptive statistics purpose, convert the statistics back to zero basis scale
        Returns simple historical covariance matrix

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov_calculator_intuitive.results["cov"]

    @property
    def cov_intuitive(self):
        """
        For descriptive statistics purpose, convert the statistics back to zero basis scale
        Returns simple historical covariance matrix

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov_calculator_intuitive.results["cov"]

    @property
    def is_valid(self):
        """
        check if inputs are valid

        Parameter
        ---------

        Return
        ------
        bool
            True if inputs are valid, false otherwise
        """
        return self.cov_calculator.is_valid()

    def assign(self, date, price_return, annualize_factor=1.0):
        """
        Transform to log scale and assign returns to the actual calculator
        Parameter
        ---------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency

        Return
        ------
        """
        annualized_return = annualize_adjustments.compound_annualization(
            price_return, annualize_factor
        )
        annualized_return = np.squeeze(annualized_return)
        self.cov_calculator.update(np.log(annualized_return + 1), index=date)
        self.cov_calculator_intuitive.update(annualized_return, index=date)
        return

    def get_portfolio_risk(self, allocation):
        """
        calculate 0 basis return and risk
        descriptive statistics of log normal distribution is the same as the original distribution

        Parameter
        ---------
        allocation: np.array
            allocation factor at the order of factors

        Return
        ------
        float
            portfolio risk

        """
        portfolio_risk, marginal_risk = portfolio_stats.portfolio_vol(
            allocation, self.risk_metrics_intuitive
        )
        return portfolio_risk
