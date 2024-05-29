import pandas as pd
import datetime
import abc
import quantkit.bt.core_structure.algo as algo


class RunPeriod(algo.Algo):
    """
    Core building block for frequency running algorithms

    Parameters
    ----------
    run_on_first_date: bool, optional
        run first time the Algo is called
    run_on_end_of_period: bool, optional
        run on end of period
    run_on_last_date: bool, optional
        run last time the Algo is called
    """

    def __init__(
        self,
        run_on_first_date: bool = True,
        run_on_end_of_period: bool = False,
        run_on_last_date: bool = False,
    ) -> None:
        super().__init__()
        self._run_on_first_date = run_on_first_date
        self._run_on_end_of_period = run_on_end_of_period
        self._run_on_last_date = run_on_last_date

    def __call__(self, target) -> bool:
        """
        Run Algo on call RunPeriod()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            frequency property is fulfilled
        """
        now = target.now
        inow = target.inow
        result = False

        # index 0 is a date added by the Backtest Constructor
        if inow == 0:
            return False
        # first date
        if inow == 1:
            if self._run_on_first_date:
                result = True
        # last date
        elif inow == (len(target.data.index) - 1):
            if self._run_on_last_date:
                result = True
        else:
            now = pd.Timestamp(now)

            index_offset = -1
            if self._run_on_end_of_period:
                index_offset = 1

            date_to_compare = target.data.index[inow + index_offset]
            date_to_compare = pd.Timestamp(date_to_compare)

            result = self.compare_dates(now, date_to_compare)

        return result

    @abc.abstractmethod
    def compare_dates(self, now: datetime.date, date_to_compare: datetime.date) -> bool:
        """
        Compare two dates and check if frequency property is fulfilled

        Parameters
        ----------
        now: datetime.date
            current date
        date_to_compare: datetime.date
            next (run on end of period) or previous (run on beginning of period) date

        Returns
        -------
        bool:
            frequency property is fulfilled
        """
        raise (NotImplementedError("RunPeriod Algo is an abstract class!"))
