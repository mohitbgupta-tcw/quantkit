import quantkit.backtester.risk_calc.risk_metrics as risk_metrics
import quantkit.mathstats.covariance.window_covariance as window_covariance
import quantkit.utils.annualize_adjustments as annualize_adjustments
import quantkit.mathstats.portfolio_stats.volatility as volatility
import numpy as np
import datetime


class LogNormalVol(risk_metrics.RiskMetrics):
    """
    Simple Covariance Calculation assuming
        - returns are log normal distributed
        - rolling historical window

    Parameters
    ----------
    universe: list
        investment universe
    frequency: str, optional
        frequency of index return data
    """

    def __init__(self, universe: list, frequency: str = None, **kwargs) -> None:
        super().__init__(universe)
        self.frequency = frequency
        self.cov_calculator = window_covariance.WindowCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.cov_calculator_intuitive = window_covariance.WindowCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )

    @property
    def risk_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted log normal historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.cov

    @property
    def cov(self) -> np.ndarray:
        """
        Forecaseted log normal historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.cov_calculator.results["cov"]

    @property
    def risk_metrics_intuitive(self) -> np.ndarray:
        """
        For descriptive statistics purpose, convert the statistics back to zero basis scale
        Returns simple historical covariance matrix

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.cov_calculator_intuitive.results["cov"]

    @property
    def cov_intuitive(self) -> np.ndarray:
        """
        For descriptive statistics purpose, convert the statistics back to zero basis scale
        Returns simple historical covariance matrix

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.cov_calculator_intuitive.results["cov"]

    def assign(
        self, date: datetime.date, price_return: np.ndarray, annualize_factor: int = 1.0
    ) -> None:
        """
        Transform to log scale and assign returns to the actual calculator

        Parameters
        ----------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency
        """
        annualized_return = annualize_adjustments.compound_annualization(
            price_return, annualize_factor
        )
        annualized_return = np.squeeze(annualized_return)
        self.cov_calculator.update(np.log(annualized_return + 1), index=date)
        # self.cov_calculator_intuitive.update(annualized_return, index=date)

    def get_portfolio_risk(self, allocation: np.ndarray) -> float:
        """
        calculate 0 basis return and risk
        descriptive statistics of log normal distribution is the same as the original distribution

        Parameters
        ----------
        allocation: np.array
            allocation factor at the order of factors

        Returns
        -------
        float
            portfolio risk

        """
        portfolio_risk = volatility.portfolio_vol(
            allocation, self.risk_metrics_intuitive
        )
        return portfolio_risk

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.cov_calculator.is_valid()
