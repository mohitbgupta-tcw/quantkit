import quantkit.asset_allocation.risk_calc.risk_metrics as risk_metrics
import quantkit.utils.annualize_adjustments as annualize_adjustments
import quantkit.mathstats.covariance.simple_covariance as simple_covariance
import quantkit.mathstats.covariance.window_covariance as window_covariance
import numpy as np


class SimpleVol(risk_metrics.RiskMetrics):
    """Simple historical Covariance Calculation of Return
    Maybe useful for factor risk calculation
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
        self.cov_calculator = simple_covariance.Covariance(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.window_cov_calculator = window_covariance.WindowCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )

    @property
    def risk_metrics_optimizer(self):
        """
        Forecaseted simple historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov

    @property
    def risk_metrics_optimizer_window(self):
        """
        Forecaseted rolling historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.window_cov

    @property
    def risk_metrics_intuitive(self):
        """
        Forecaseted simple historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov

    @property
    def risk_metrics_intuitive_window(self):
        """
        Forecaseted rolling historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.window_cov

    @property
    def cov(self):
        """
        Forecaseted simple historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov_calculator.results["cov"]

    @property
    def window_cov(self):
        """
        Forecaseted rolling historical covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.window_cov_calculator.results["cov"]

    @property
    def volatility(self):
        """
        Historical volatility based on diagonal of simple historical covariance matrix

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return np.sqrt(np.diag(self.cov_calculator.results["cov"]))

    @property
    def window_volatility(self):
        """
        Historical volatility based on diagonal of rolling historical covariance matrix

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return np.sqrt(np.diag(self.window_cov_calculator.results["cov"]))

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
        return self.window_cov_calculator.is_valid()

    def assign(self, date, price_return, annualize_factor=1.0):
        """
        Transform and assign returns to the actual calculator
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
        annualized_return = annualize_adjustments.compound_annualize(
            price_return, annualize_factor
        )
        annualized_return = np.squeeze(annualized_return)

        self.cov_calculator.update(annualized_return, index=date)
        self.window_cov_calculator.update(annualized_return, index=date)
        return

    def get_portfolio_risk(self, allocation, is_window=True):
        """
        calculate 0 basis portfolio risk

        Parameter
        ---------
        allocation: np.array
            allocation factor at the order of factors

        Return
        ------
        float
            portfolio risk

        """
        risk_metrics = (
            self.risk_metrics_optimizer_window
            if is_window
            else self.risk_metrics_optimizer
        )
        portfolio_risk, marginal_risk = portfolio_stats.portfolio_vol(
            allocation, risk_metrics
        )
        return portfolio_risk
