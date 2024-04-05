import pandas as pd
import datetime
import quantkit.bt.core_structure.algo as algo


class RunOnDate(algo.Algo):
    """
    Algorithm to determine if current date is in specific set of dates.

    Parameters
    ----------
    dates: list
        list of dates
    """

    def __init__(self, *dates) -> None:
        super().__init__()
        self.dates = [pd.to_datetime(d) for d in dates]

    def __call__(self, target) -> bool:
        """
        Run Algo on call RunOnDate()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            date is in set of dates
        """
        return target.now in self.dates


class RunAfterDate(algo.Algo):
    """
    Algorithm to determine if specific date has passed
    Useful for Algos that rely on trailing averages
    -> don't want to start trading until some amount of data has been built up

    Parameters
    ----------
    date: datetime.date
        date after which to start trading
    """

    def __init__(self, date: datetime.date) -> None:
        super().__init__()
        self.date = pd.to_datetime(date)

    def __call__(self, target) -> bool:
        """
        Run Algo on call RunAfterDate()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            date is after start date
        """
        return target.now > self.date


class RunAfterDays(algo.Algo):
    """
    Algorithm to determine if specific number of days have passed
    Useful for Algos that rely on trailing averages
    -> don't want to start trading until some amount of data has been built up

    Parameters
    ----------
    days: int
        number of days after which to start trading
    """

    def __init__(self, days: int) -> None:
        super().__init__()
        self.days = days

    def __call__(self, target) -> bool:
        """
        Run Algo on call RunAfterDays()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            date is after start date
        """
        if self.days > 0:
            self.days -= 1
            return False
        return True


class RunOnce(algo.Algo):
    """
    Algorithm to determine if we are on first day
    Useful in situations where we want to run the logic once only
    -> Buy and Hold
    """

    def __init__(self) -> None:
        super().__init__()
        self.has_run = False

    def __call__(self, target) -> bool:
        """
        Run Algo on call RunOnce()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            first day
        """
        inow = target.inow

        # index 0 is a date added by the Backtest Constructor
        if inow == 0:
            return False
        if not self.has_run:
            self.has_run = True
            return True
        return False


class RunEveryNPeriods(algo.Algo):
    """
    Algorithm to determine if reoccurring n periods have passed

    Example
    -------
    - each month select top 5 performers
    - hold for 3 months
    - create 3 strategies with different offsets
    - create master strategy that allocates equal amounts of capital to every sub-strategy

    Parameters
    ----------
    n: int
        run every n periods
    offset: int, optional
        offset for first run
    """

    def __init__(self, n: int, offset: int = 0) -> None:
        super().__init__()
        self.n = n
        self.offset = offset
        self.idx = n - offset

    def __call__(self, target) -> bool:
        """
        Run Algo on call RunEveryNPeriods()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            n periods are over
        """
        inow = target.inow

        # index 0 is a date added by the Backtest Constructor
        if inow == 0:
            return False

        if self.idx == self.n:
            self.idx = 1
            return True
        else:
            self.idx += 1
            return False
