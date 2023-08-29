class RiskMetrics(object):
    """Base class for building risk metrics"""

    def __init__(self, universe):
        """
        Parameter
        ---------
        factors: list
            factors to run risk calculation on
        """
        self.universe = universe
        self.universe_size = len(universe)

    @property
    def risk_metrics_optimizer(self):
        """
        Forecaseted covariance matrix from risk engine

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        raise NotImplementedError

    @property
    def risk_metrics_intuitive(self):
        """
        risk metrics for plotting needs to be human interpretable

        Parameter
        ---------

        Return
        ------
        2-D <np.array>
            covariance matrix
        """
        raise NotImplementedError

    def get_portfolio_risk(self, allocation):
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
        raise NotImplementedError

    def assign(self, date, price_return, annualize_factor=None):
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
        raise NotImplementedError
