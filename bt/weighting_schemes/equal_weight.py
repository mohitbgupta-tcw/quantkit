import quantkit.bt.core_structure.algo as algo


class EqualWeight(algo.Algo):
    """
    Calculate Equal Weight Allocation
    -> set temp["weights"] by calculating equal weights for all items in selected
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call EqualWeight()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        selected = target.temp["selected"]
        n = len(selected)

        if n == 0:
            target.temp["weights"] = {}
        else:
            w = 1.0 / n
            target.temp["weights"] = {x: w for x in selected}

        return True


class EqualNotionalWeight(algo.Algo):
    """
    Calculate Equal Notional Weight Allocation
    -> set temp["weights"] by calculating equal notional weights for all items in selected
    -> useful in fixed income setting when you want equal notional weighting, not market weighting
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call EqualWeight()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        selected = target.temp["selected"]

        if not selected:
            target.temp["weights"] = {}
        else:
            price_sum = sum([target.children[sec]._price for sec in selected])
            target.temp["weights"] = {
                target.children[sec].name: target.children[sec]._price / price_sum
                for sec in selected
            }
        return True
