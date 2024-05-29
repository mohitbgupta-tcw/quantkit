import quantkit.bt.core_structure.algo as algo


class CapitalFlow(algo.Algo):
    """
    Model capital flows. Flows can either be inflows or outflows.
    Algo will affect the capital of the target Node without affecting returns
    It's modeled as an adjustment
    -> the capital will remain in the strategy until a re-allocation/rebalancement is made

    Example
    -------
    - pension fund with inflows every month or year due to contributions

    Parameters
    ----------
    amount: float
        amount of adjustment

    """

    def __init__(self, amount: float) -> None:
        super().__init__()
        self.amount = float(amount)

    def __call__(self, target) -> bool:
        """
        Run Algo on call CapitalFlow() and pass/ withdraw money to/ from the strategy

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        target.adjust(self.amount, flow=True)
        return True
