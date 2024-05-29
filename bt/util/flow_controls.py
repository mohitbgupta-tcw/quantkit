import quantkit.bt.core_structure.algo as algo


class Or(algo.Algo):
    """
    Combine multiple signals into one signal and check if one returns True

    Example
    -------
    - two different rebalance signals:
        - runOnDateAlgo = RunOnDate(datetime.date(1997, 1, 6))
        - runMonthlyAlgo = bt.algos.RunMonthly()
    - Or([runMonthlyAlgo, runOnDateAlgo])
    - OrAlgo will return True if it is the Jan-6-1997 OR if it is 1st of the month

    Parameters
    ----------
    list_of_algos: list
        list of algos
    """

    def __init__(self, list_of_algos: list) -> None:
        super().__init__()
        self._list_of_algos = list_of_algos

    def __call__(self, target) -> bool:
        """
        Run Algo on call Or() and return True if any Algo returns True

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            any Algo returns True
        """
        res = False
        for algo in self._list_of_algos:
            tempRes = algo(target)
            res = res or tempRes
        return res


class Not(algo.Algo):
    """
    Invert signal of other flow control algo

    Parameters
    ----------
    algorithm: Algo
        algo to run and invert signal of
    """

    def __init__(self, algorithm: algo.Algo) -> None:
        super().__init__()
        self._algo = algorithm

    def __call__(self, target) -> bool:
        """
        Run Algo on call Not()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            invertion of algo
        """
        return not self._algo(target)
