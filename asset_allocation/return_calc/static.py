import quantkit.asset_allocation.return_calc.return_metrics as return_metrics
import quantkit.utils.annualize_adjustments as annualize_adjustments
import quantkit.utils.mapping_configs as mapping_utils
import pandas as pd
import numpy as np


class StaticReturn(return_metrics.ReturnMetrics):
    """Return Calculation Assuming User Input Returns"""

    def __init__(self, factors, frequency=None, **kwargs):
        """
        Parameter
        ---------
        factors: list
            factors to run return calculation on
        frequency: str, optional
            frequency of index return data
        """
        super().__init__(factors)
        self.frequency = frequency
        self.annualize_factor = mapping_utils.annualize_factor_d.get(frequency, 252)
        self.current_returns = None
        self._returns_df = None
        self._create_returns(**kwargs)

    def _create_returns(self, returns_csv=None, returns_d=None, **kwargs):
        """
        Read user input and convertion from annual to monthly returns
        !! CSV has higher priority than a static dictionary

        Parameter
        ---------
        returns_csv: str, optional
            path to local csv file
        returns_d: dict, optional
            dictionary mapping returns to each asset
        """
        if returns_csv is None:
            exp_returns = []
            for this_factor in self.factors:
                this_return = returns_d.get(this_factor)
                exp_returns.append(this_return)

            self.current_returns = annualize_adjustments.compound_annualize(
                np.array(exp_returns), 1 / self.annualize_factor
            )
            return

        self._returns_df = pd.read_csv(returns_csv, index_col=0)[list(self.factors)]
        self._returns_df.index = pd.to_datetime(self._returns_df.index).date
        return

    @property
    def return_metrics_optimizer(self):
        """
        User Input Returns

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        return self.current_returns

    @property
    def return_metrics_intuitive(self):
        """
        User Input Returns

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        return self.current_returns

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
        return True

    def get_portfolio_return(self, allocation, **kwargs):
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameter
        ---------
        allocation: np.array
            current allocation

        Return
        ------
        pd.DataFrame
            return: float

        """
        return super().get_portfolio_return(allocation, self.current_returns, **kwargs)

    def assign(
        self,
        date,
        price_return,
        market_weights=None,
        risk_free_rate=None,
        annualize_factor=1.0,
    ):
        """
        Transform and assign returns to the actual calculator
        Parameter
        ---------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        market_weights: np.array, optional
            market weights for each asset
        risk_free_rate: float, optional
            risk free rate
        annualize_factor: int, optional
            factor depending on data frequency

        Return
        ------
        """
        if self._returns_df is None:
            return

        exp_returns = self._returns_df.loc[date].tolist()
        self.current_returns = annualize_adjustments.compound_annualize(
            np.array(exp_returns), 1 / self.annualize_factor
        )

        return
