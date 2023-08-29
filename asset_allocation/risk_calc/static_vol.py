import quantkit.asset_allocation.risk_calc.risk_metrics as risk_metrics
import quantkit.mathstats.covariance.window_covariance as window_covariance
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import pandas as pd


class StaticVol(risk_metrics.RiskMetrics):
    """Covariance Calculation assuming the user puts in his own standard deviations"""

    def __init__(self, factors, frequency=None, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run risk calculation on
        frequency: str, optional
            frequency of index return data
        """
        super().__init__(factors)
        self.frequency = frequency
        self.cov_calculator = window_covariance.WindowCovariance(
            num_ind_variables=self.universe_size, **kwargs
        )
        self.current_stds = None
        self._std_devs_df = None
        self._create_stds(**kwargs)

    def _create_stds(self, std_devs_csv=None, std_devs_d=None, **kwargs):
        """
        Read user input and convertion from annual to monthly standard deviations
        !! CSV has higher priority than a static dictionary

        Parameter
        ---------
        std_devs_csv: str, optional
            path to local csv file
        std_devs_d: dict, optional
            dictionary mapping std devs to each asset
        """
        if std_devs_csv is None:
            exp_std_devs = []
            for this_factor in self.factors:
                this_std_dev = std_devs_d.get(this_factor)
                exp_std_devs.append(this_std_dev)

            self.current_stds = annualize_adjustments.volatility_de_annualize(
                np.array(exp_std_devs), self.annualize_factor
            )
            return

        self._std_devs_df = pd.read_csv(std_devs_csv, index_col=0)[list(self.factors)]
        self._std_devs_df.index = pd.to_datetime(self._std_devs_df.index).date
        return

    @property
    def risk_metrics_optimizer(self):
        """
        Covariance matrix from user input

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
        Covariance matrix from user input
        Use correlation to calculate non diagonal entries

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        c = self.cov_calculator.results["cov"]
        corr = matrix.cov_to_corr(c)
        return matrix.corr_to_cov(corr, self.current_stds)

    @property
    def risk_metrics_intuitive(self):
        """
        For descriptive statistics purpose, convert the statistics back to zero basis scale

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov_intuitive

    @property
    def cov_intuitive(self):
        """
        Covariance matrix from user input

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        return self.cov

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
        if any(np.isnan(price_return)):
            return

        if self._std_devs_df is not None:
            exp_std_devs = self._std_devs_df.loc[date].tolist()
            self.current_stds = annualize_adjustments.volatility_de_annualize(
                np.array(exp_std_devs), self.annualize_factor
            )

        annualized_return = annualize_adjustments.compound_annualize(
            price_return, annualize_factor
        )
        annualized_return_np = annualized_return

        self.cov_calculator.update(annualized_return, index=date)
        return

    def get_portfolio_risk(self, allocation, is_window=True):
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
        risk_metrics = (
            self.risk_metrics_optimizer if is_window else self.risk_metrics_optimizer
        )
        portfolio_risk, marginal_risk = portfolio_stats.portfolio_vol(
            allocation, risk_metrics
        )
        return portfolio_risk
