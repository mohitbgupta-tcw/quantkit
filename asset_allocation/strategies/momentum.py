import quantkit.asset_allocation.strategies.strategy as strategy
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np


class Momentum(strategy.Strategy):
    """
    "buy low, sell high."
    """

    def __init__(self, params):
        super().__init__(**params)
        self.window_size = params["window_size"]
        self.top_n = params["top_n"]

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
        self.return_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )
        if date in self.rebalance_dates:
            self.risk_engine.assign(
                date=date, price_return=price_return, annualize_factor=annualize_factor
            )

    @property
    def selected_securities(self) -> np.array:
        """
        Index of top n momentum returns

        Parameter
        ---------

        Return
        ------
        <np.array>
            index
        """
        return (-self.return_metrics_intuitive).argsort()[: self.top_n]

    @property
    def return_metrics_optimizer(self):
        """
        Forecaseted DAILY returns from return engine of top n momentum returns

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        returns_topn = self.return_metrics_intuitive[self.selected_securities]
        return annualize_adjustments.compound_annualize(returns_topn, 1 / self.top_n)
