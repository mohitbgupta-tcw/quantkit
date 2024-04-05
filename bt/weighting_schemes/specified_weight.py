import pandas as pd
import quantkit.bt.core_structure.algo as algo


class SpecifiedWeight(algo.Algo):
    """
    Use pre-specified target weights
    -> set temp["weights"] based on provided dict of ticker:weights

    Example
    -------
    weights =
    foo    0.6
    bar    0.4
    rf     0.0

    SpecifiedWeight(**weights)

    Parameters
    ----------
    weights: dict
        target weights -> ticker: weight
    """

    def __init__(self, **weights) -> None:
        super().__init__()
        self.weights = weights

    def __call__(self, target) -> bool:
        """
        Run Algo on call SpecifiedWeight()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        tw = self.weights.copy()
        tw = {sec: weight for sec, weight in tw.items() if weight != 0}
        target.temp["weights"] = tw
        return True


class TargetWeight(algo.Algo):
    """
    Set target weights based on target weight DataFrame
    -> target weights are set on every date provided in DataFrame (could be daily, monthly, etc.)
    -> pass weights into additional_data when initializing backtest

    Parameters
    ----------
    weights: str
        name of DataFrame containing the target weights
    """

    def __init__(self, weights: str) -> None:
        super().__init__()
        self.weights_name = weights

    def __call__(self, target) -> bool:
        """
        Run Algo on call TargetWeight()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            weighting done
        """
        # get current target weights
        weights = target.get_data(self.weights_name)

        if target.now in weights.index:
            w = weights.loc[target.now]
            target.temp["weights"] = w.dropna().to_dict()
            return True
        else:
            return False
