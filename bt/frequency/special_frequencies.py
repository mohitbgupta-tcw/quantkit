import quantkit.bt.core_structure.algo as algo


class RunIfOutOfBounds(algo.Algo):
    """
    Algorithm to determine if one of security weights (or cash) deviates by an amount greater than tolerance percent

    Example
    -------
    - tolerance is set to 0.5
    - Security foo: target weight of 0.2
    - Security foo: because of good performance weight grows to greater than 0.3

    Usage
    -----
    - strategy rebalance should be performed:
        - quarterly
        - whenever any security's weight deviates by more than 20%
    - Or([runQuarterlyAlgo,runIfOutOfBoundsAlgo(0.2)])

    Parameters
    ----------
    tolerance: float
        allowed deviation of each security weight
    """

    def __init__(self, tolerance: float) -> None:
        self.tolerance = float(tolerance)
        super().__init__()

    def __call__(self, target) -> bool:
        """
        Run Algo on call RunIfOutOfBounds()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            one security (or cash) deviates by more than tolerance
        """
        if "weights" not in target.temp:
            return False

        targets = target.temp["weights"]

        for child in target.children:
            if child in targets:
                c = target.children[child]
                deviation = abs((c.weight - targets[child]) / targets[child])
                if deviation > self.tolerance:
                    # print(target.now, c.name, c.weight, targets[child])
                    return True

        if "cash" in target.temp:
            cash_deviation = abs(
                (target.capital - targets.value) / targets.value - target.temp["cash"]
            )
            if cash_deviation > self.tolerance:
                return True
        return False
