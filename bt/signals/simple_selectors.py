import random
import pandas as pd
from typing import Union
import quantkit.bt.core_structure.algo as algo


class SelectHasData(algo.Algo):
    """
    Select securities that meet data requirements in a pre-defined lookback length
    -> save them in temp["selected"]
    -> securities without NAN's
    -> by default, exclude securities without price on current date or those with faulty price
    Useful for selecting tickers that need a certain amount of data for future algos to run.

    Parameters
    ----------
    lookback: int
        length of lookback window
    include_negative: bool, optional
        include securities with negative or zero prices
    """

    def __init__(
        self,
        lookback: int,
        include_negative: bool = False,
    ) -> None:
        super().__init__()
        self.lookback = lookback
        self.include_negative = include_negative

    def __call__(self, target) -> bool:
        """
        Run Algo on call SelectHasData() and set temp["selected"] with selected securities

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "selected" in target.temp:
            selected = target.temp["selected"]
        else:
            selected = target.universe.columns

        filt = target.universe[target.inow - self.lookback :][selected].dropna(axis=1)
        if not self.include_negative:
            filt = filt[filt > 0].dropna(axis=1)
        target.temp["selected"] = list(filt.columns)
        return True


class SelectAll(SelectHasData):
    """
    Select all securities in universe.
    -> save them in temp["selected"]
    -> by default, exclude securities without price on current date or those with faulty price

    Parameters
    ----------
    include_negative: bool, optional
        include securities with negative or zero prices
    """

    def __init__(self, include_negative: bool = False) -> None:
        super().__init__(lookback=0, include_negative=include_negative)


class SelectRandomly(algo.AlgoStack):
    """
    Select n random securities from universe
    -> save them in temp["selected"]
    -> by default, exclude securities without price on current date or those with faulty price
    Useful for random benchmarking

    Parameters
    ----------
    n: int
        number of securities to select
    include_negative: bool, optional
        include securities with negative or zero prices
    """

    def __init__(self, n: int, include_negative: bool = False) -> None:
        super().__init__()
        self.n = n
        self.include_negative = include_negative

    def __call__(self, target) -> bool:
        """
        Run Algo on call SelectRandomly() and set temp["selected"] with n random securities

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "selected" in target.temp:
            sel = target.temp["selected"]
        else:
            sel = list(target.universe.columns)

        universe = target.universe.loc[target.now, sel].dropna()
        if self.include_negative:
            sel = list(universe.index)
        else:
            sel = list(universe[universe > 0].index)

        n = self.n if self.n < len(sel) else len(sel)
        sel = random.sample(sel, n)

        target.temp["selected"] = sel
        return True


class SelectActive(algo.Algo):
    """
    Select all active securities
    -> save them in temp["selected"]
    -> exclude securities that already have been closed (ClosePositionsAfterDates)
        or rolled (RollPositionsAfterDates)
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call SelectActive() and set temp["selected"] active securities

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "selected" in target.temp:
            selected = target.temp["selected"]
        else:
            selected = list(target.universe.columns)
        rolled = target.perm.get("rolled", set())
        closed = target.perm.get("closed", set())
        selected = [s for s in selected if s not in set.union(rolled, closed)]
        target.temp["selected"] = selected
        return True


class SelectThese(algo.Algo):
    """
    Select securities from a pre-defined list of tickers
    -> save them in temp["selected"]
    -> by default, exclude securities without price on current date or those with faulty price

    Parameters
    ----------
    ticker: list
        list of selected tickers
    include_negative: bool, optional
        include securities with negative or zero prices
    """

    def __init__(self, tickers: list, include_negative: bool = False) -> None:
        super().__init__()
        self.tickers = tickers
        self.include_negative = include_negative

    def __call__(self, target) -> bool:
        """
        Run Algo on call SelectThese() and set temp["selected"] with selected securities

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "selected" in target.temp:
            sel = target.temp["selected"]
        else:
            sel = list(target.universe.columns)

        tickers = list(set(sel) & set(self.tickers))

        universe = target.universe.loc[target.now, tickers].dropna()
        if self.include_negative:
            target.temp["selected"] = list(universe.index)
        else:
            target.temp["selected"] = list(universe[universe > 0].index)
        return True


class SelectN(algo.Algo):
    """
    Select top or bottom n securities based on ranking temp["stat"]
    -> save them in temp["selected"]
    -> temp["stat"] is metric that should be computed in previous Algo

    Parameters
    ----------
    n: int | float
        number or percentage of securities selected
    sort_descending: bool, optional
        Sort stat descending (highest is best)
    all_or_none: bool, optional
        populate result only if more than n securities are available
    """

    def __init__(
        self,
        n: Union[int, float],
        sort_descending: bool = True,
        all_or_none: bool = False,
    ) -> None:
        super().__init__()
        self.n = n
        self.ascending = not sort_descending
        self.all_or_none = all_or_none

    def __call__(self, target) -> bool:
        """
        Run Algo on call SelectN() and set temp["selected"] with n selected securities

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        stat = target.temp["stat"].dropna()

        if "selected" in target.temp:
            stat = stat.loc[stat.index.intersection(target.temp["selected"])]
        stat = stat.sort_values(ascending=self.ascending)

        # handle percent n
        keep_n = self.n
        if self.n < 1:
            keep_n = int(self.n * len(stat))

        sel = list(stat[:keep_n].index)

        if self.all_or_none and len(sel) < keep_n:
            sel = []

        target.temp["selected"] = sel

        return True


class SelectWhere(algo.Algo):
    """
    Select securities based on indicator DataFrame.
    -> save them in temp["selected"]
    -> select securities where value is True on current date
    -> by default, exclude securities without price on current date or those with faulty price

    Parameters
    ----------
    signal: pd.DataFrame
        Boolean DataFrame containing selection logic
    include_negative: bool, optional
        include securities with negative or zero prices
    """

    def __init__(
        self, signal: Union[str, pd.DataFrame], include_negative: bool = False
    ) -> None:
        super().__init__()
        self.signal = signal
        self.include_negative = include_negative

    def __call__(self, target) -> bool:
        """
        Run Algo on call SelectWhere() and set temp["selected"]

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "selected" in target.temp:
            sel = target.temp["selected"]
        else:
            sel = list(target.universe.columns)

        if target.now in self.signal.index:
            sig = self.signal.loc[target.now]
            selected = sig[sig == True].index

            tickers = list(set(sel) & set(selected))

            universe = target.universe.loc[target.now, tickers].dropna()
            if self.include_negative:
                selected = list(universe.index)
            else:
                selected = list(universe[universe > 0].index)
            target.temp["selected"] = list(selected)
        else:
            raise ValueError(f"{target.now} not in Signal DataFrame.")
        return True
