import quantkit.asset_allocation.strategies.strategy as strategy


class Momentum(strategy.Strategy):
    """
    "buy low, sell high."
    """

    def __init__(self, params):
        super().__init__(**params)
        self.window_size = params["window_size"]
