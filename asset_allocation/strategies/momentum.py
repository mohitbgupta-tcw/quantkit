import quantkit.asset_allocation.strategies.strategy as strategy


class Momentum(strategy.Strategy):
    """
    "buy low, sell high."
    """

    def __init__(self, params):
        super().__init__(**params)
        self.window_size = params["window_size"]

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
