class Algo(object):
    """
    Modularize strategy logic

    Algos are a function that receive the strategy as argument (referred to as target),
    and are expected to return a bool.

    Parameters
    ----------
        name: str
            name of algorithm
    """

    def __init__(self, name=None) -> None:
        self._name = name
        self.run_always = False

    @property
    def name(self) -> str:
        """
        Returns
        -------
        str:
            Algo name
        """
        if self._name is None:
            self._name = self.__class__.__name__
        return self._name

    def __call__(self, target) -> bool:
        """
        Run Algo on call Algo()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            Algo logic fails or not
        """
        raise NotImplementedError("%s not implemented!" % self.name)


class AlgoStack(Algo):
    """
    Run multiple Algos until failure
    -> group logic set of Algos together.
    -> each Algo in stack is run
    -> execution stops if one Algo returns False

    Parameters
    ----------
    algos: list
        list of algos
    """

    def __init__(self, *algos) -> None:
        super(AlgoStack, self).__init__()
        self.algos = algos

    def __call__(self, target) -> bool:
        """
        Run all Algos in stack as long as True
        After False is returned, only run necessary Algos

        Parameters
        ----------
        target: Strategy
            Strategy

        Returns
        -------
        bool:
            ran all Algos in stack
        """
        res = True
        for algo in self.algos:
            if res:
                res = algo(target)
            elif algo.run_always:
                algo(target)
        return res
