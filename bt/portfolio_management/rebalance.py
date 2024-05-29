import numpy as np
import pandas as pd
import quantkit.bt.core_structure.algo as algo


class Rebalance(algo.Algo):
    """
    Rebalance capital based on temp["weights"]
    -> close positions if open but not in target_weights.
    -> typically last Algo called once target weights have been set
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call Rebalance()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            rebalance done
        """
        if "weights" not in target.temp:
            return True

        targets = target.temp["weights"]
        base = target.value

        # de-allocate children that are not in targets and have position
        for cname in target.children:
            if cname in targets:
                continue

            c = target.children[cname]
            v = c.value

            if v != 0.0 and not np.isnan(v):
                target.close(cname)

        if "cash" in target.temp:
            base = base * (1 - target.temp["cash"])

        for item in targets.items():
            target.rebalance(child=item[0], weight=item[1], base=base)
        return True


class RebalanceOverTime(algo.Algo):
    """
    Rebalance to target weight over n periods (days)
    -> some strategies can produce large swings in allocations
    -> split up the weight delta over n periods

    Parameters
    ----------
    n: int
        number of periods (days) over which rebalancing takes place
    """

    def __init__(self, n: int) -> None:
        super().__init__()
        self.run_always = True
        self.n = float(n)
        self._rb = Rebalance()
        self._weights = None
        self._days_left = None

    def __call__(self, target) -> bool:
        """
        Run Algo on call RebalanceOverTime()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            rebalance done
        """
        if "weights" in target.temp:
            self._weights = target.temp["weights"]
            self._days_left = self.n

        if self._weights is not None:
            tgt = {}
            for cname in self._weights.keys():
                curr = (
                    target.children[cname].weight if cname in target.children else 0.0
                )
                dlt = (self._weights[cname] - curr) / self._days_left
                tgt[cname] = curr + dlt

            target.temp["weights"] = tgt
            self._rb(target)

            self._days_left -= 1

            if self._days_left == 0:
                self._days_left = None
                self._weights = None

        return True
