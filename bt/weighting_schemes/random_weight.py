import random
import quantkit.bt.core_structure.algo as algo


# TODO make RandomOptimizer class similar to quantkit
def random_weights(n: int, bounds=(0.0, 1.0)) -> list:
    """
    Generate pseudo-random weights.

    Parameters
    ----------
    n: int
        number of random weights
    bounds: tuple, optional
        tuple including low and high bounds for each security

    Returns
    -------
    list:
        list of random weights
    """
    total = 1
    low = bounds[0]
    high = bounds[1]

    if n * high < total or n * low > total:
        raise ValueError("solution not possible with given n and bounds")

    w = [0] * n
    tgt = -float(total)

    for i in range(n):
        rn = n - i - 1
        rhigh = rn * high
        rlow = rn * low

        lowb = max(-rhigh - tgt, low)
        highb = min(-rlow - tgt, high)

        rw = random.uniform(lowb, highb)
        w[i] = rw

        tgt += rw

    random.shuffle(w)
    return w


class RandomWeight(algo.Algo):
    """
    Calculate Random Weight Allocation
    -> set temp["weights"] based on a random weight vector
    -> useful for benchmarking against strategy where we believe weighting algorithm is adding value

    Parameters
    ----------
    bounds: tuple, optional
        tuple including low and high bounds for each security
    """

    def __init__(self, bounds=(0.0, 1.0)):
        super().__init__()
        self.bounds = bounds

    def __call__(self, target) -> bool:
        """
        Run Algo on call WeighRandomly()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        sel = target.temp["selected"]
        n = len(sel)

        rw = random_weights(n, self.bounds)
        w = dict(zip(sel, rw))

        target.temp["weights"] = w
        return True
