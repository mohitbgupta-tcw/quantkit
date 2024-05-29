import numpy as np
import pandas as pd
import quantkit.bt.core_structure.algo as algo


class ScaleWeights(algo.Algo):
    """
    Apply leverage and scale weights
    -> has to be run after other weighting Algo
    -> set temp["weights"] based on a scaled version of itself

    Parameters
    ----------
    leverage: float
        scaling factor
    """

    def __init__(self, leverage: float) -> None:
        super(ScaleWeights, self).__init__()
        self.leverage = leverage

    def __call__(self, target):
        """
        Run Algo on call ScaleWeights()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        target.temp["weights"] = {
            k: self.leverage * w for k, w in target.temp["weights"].items()
        }
        return True


class LimitWeights(algo.Algo):
    """
    Limit the weight of any one specifc asset
    -> set temp['weights'] based on weight limits
    -> excess weight is redistributed to other assets, proportionally to their current weights

    Example
    -------
    - weights are {a: 0.7, b: 0.2, c: 0.1}
    - limit=0.5
    - excess 0.2 in a is ditributed to b and c proportionally
    - result is {a: 0.5, b: 0.33, c: 0.167}

    Parameters
    ----------
    limit: float
        weight limit
    """

    def __init__(self, limit: float) -> None:
        super().__init__()
        self.limit = limit

    def _limit_weights(self, weights: dict):
        """
        Limits weights and redistributes excedent amount proportionally

        Parameters
        ----------
        weights: dict
            A series describing the weights

        Returns
        --------
        """
        weights = pd.Series(weights)

        to_rebalance = (weights[weights > self.limit] - self.limit).sum()
        ok = weights[weights < self.limit]
        ok += (ok / ok.sum()) * to_rebalance

        weights[weights > self.limit] = self.limit
        weights[weights < self.limit] = ok

        if any(x > self.limit for x in weights):
            return self._limit_weights(weights.to_dict())
        return weights.to_dict()

    def __call__(self, target) -> bool:
        """
        Run Algo on call LimitWeights()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        tw = target.temp["weights"]
        if len(tw) == 0:
            return True

        if self.limit < 1.0 / len(tw):
            raise ValueError("invalid limit -> 1 / limit must be <= len(weights)")
        else:
            tw = self._limit_weights(tw)
        target.temp["weights"] = tw
        return True


class LimitDeltas(algo.Algo):
    """
    Restrict how much a security's target weight can change from rebalance to rebalance.
    -> set temp["weights"] based on weight delta limits

    Example
    -------
    - long 100% in security a
    - new weight of a: 0%
    - limit of 0.1
    - new target weight will be 90% instead of 0%

    Parameters
    -----------
    limit: float
        Weight delta limit for all securities
    """

    def __init__(self, limit: float) -> None:
        super().__init__()
        self.limit = limit

    def __call__(self, target) -> bool:
        """
        Run Algo on call LimitDeltas()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        tw = target.temp["weights"]
        all_keys = list(target.children.keys())

        for k in all_keys:
            tgt = tw[k] if k in tw else 0.0
            cur = target.children[k].weight if k in target.children else 0.0
            delta = tgt - cur

            if abs(delta) > self.limit:
                if not target.inow <= 1:
                    tw[k] = cur + (self.limit * np.sign(delta))
                    print(target.now, target.children[k].name, tgt, cur, tw[k])
        return True
