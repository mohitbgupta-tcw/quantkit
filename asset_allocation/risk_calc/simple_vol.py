import quantkit.asset_allocation.risk_calc.risk_metrics as risk_metrics
import quantkit.utils.annualize_adjustments as annualize_adjustments
import quantkit.mathstats.covariance.simple_covariance as simple_covariance
import quantkit.mathstats.covariance.window_covariance as window_covariance
import quantkit.mathstats.portfolio_stats.volatility as volatility
import numpy as np
import datetime


class SimpleVol(risk_metrics.RiskMetrics):
    """
    Simple historical Covariance Calculation of Return

    Parameters
    ----------
    universe: list
        investment universe
    frequency: str, optional
        frequency of index return data
    """

    def __init__(self, universe: list, frequency: int = None, **kwargs) -> int:
        super().__init__(universe)
        self.frequency = frequency
        self.cov_calculator = simple_covariance.Covariance(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.window_cov_calculator = window_covariance.WindowCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )

    @property
    def risk_metrics_optimizer(self) -> np.array:
        """
        Forecaseted simple historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.cov

    @property
    def risk_metrics_optimizer_window(self) -> np.array:
        """
        Forecaseted rolling historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.window_cov

    @property
    def risk_metrics_intuitive(self) -> np.array:
        """
        Forecaseted simple historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.cov

    @property
    def risk_metrics_intuitive_window(self) -> np.array:
        """
        Forecaseted rolling historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.window_cov

    @property
    def cov(self) -> np.array:
        """
        Forecaseted simple historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.cov_calculator.results["cov"]

    @property
    def window_cov(self) -> np.array:
        """
        Forecaseted rolling historical covariance matrix from risk engine

        Returns
        -------
        np.array
            covariance matrix
        """
        return self.window_cov_calculator.results["cov"]

    @property
    def volatility(self) -> float:
        """
        Historical volatility based on diagonal of simple historical covariance matrix

        Returns
        ------
        float
            volatility
        """
        return np.sqrt(np.diag(self.cov_calculator.results["cov"]))

    @property
    def window_volatility(self) -> float:
        """
        Historical volatility based on diagonal of rolling historical covariance matrix

        Returns
        -------
        float
            volatility
        """
        return np.sqrt(np.diag(self.window_cov_calculator.results["cov"]))

    def assign(
        self, date: datetime.date, price_return: np.array, annualize_factor: int = 1.0
    ) -> None:
        """
        Transform and assign returns to the actual calculator

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

        self.cov_calculator.update(annualized_return, index=date)
        self.window_cov_calculator.update(annualized_return, index=date)

    def get_portfolio_risk(self, allocation: np.array, is_window: bool = True) -> float:
        """
        calculate 0 basis portfolio risk

        Parameters
        ----------
        allocation: np.array
            allocation factor at the order of factors
        is_window: bool
           calculate portfolio risk based on simple or windowed returns

        Returns
        -------
        float
            portfolio risk

        """
        risk_metrics = (
            self.risk_metrics_optimizer_window
            if is_window
            else self.risk_metrics_optimizer
        )
        portfolio_risk = volatility.portfolio_vol(allocation, risk_metrics)
        return portfolio_risk
