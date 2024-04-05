import re
import quantkit.bt.core_structure.algo as algo


class SelectRegex(algo.Algo):
    """
    Select securities based on their name
    -> save them in temp["selected"]

    Parameters
    ----------
    regex: str
        regular expression on the name
    """

    def __init__(self, regex: str) -> None:
        super().__init__()
        self.regex = re.compile(regex)

    def __call__(self, target) -> bool:
        """
        Run Algo on call SelectRegex() and set temp["selected"]

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        selected = target.temp["selected"]
        selected = [s for s in selected if self.regex.search(s)]
        target.temp["selected"] = selected
        return True
