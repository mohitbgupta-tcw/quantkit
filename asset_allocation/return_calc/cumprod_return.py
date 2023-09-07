import quantkit.asset_allocation.return_calc.return_metrics as return_metrics
import quantkit.mathstats.product.rolling_cumprod as rolling_cumprod
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np


class CumProdReturn(return_metrics.ReturnMetrics):
    """
    Cumulative Return Calculation assuming:
        - Log Normal Distribution of returns
        - Rolling Historical Window

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
        self.return_calculator = rolling_cumprod.RollingCumProd(
            num_ind_variables=self.universe_size, **kwargs
        )

    @property
    def return_metrics_optimizer(self) -> np.array:
        """
        Forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_calculator.cumprod

    @property
    def return_metrics_intuitive(self) -> np.array:
        """
        Forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_metrics_optimizer

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
        this_returns = np.exp(self.return_calculator.data_stream.values)
        this_dates = self.return_calculator.data_stream.indexes
        return super().get_portfolio_return(
            allocation, this_returns, this_dates, **kwargs
        )

    def assign(
        self,
        date,
        price_return,
        annualize_factor=1.0,
    ) -> None:
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
        annualized_return = annualize_adjustments.compound_annualization(
            price_return, annualize_factor
        )

        annualized_return = np.squeeze(annualized_return)

        outgoing_row = np.squeeze(self.return_calculator.windowed_outgoing_row)
        self.return_calculator.update(
            (np.log(annualized_return + 1) + 1), outgoing_row, index=date
        )
