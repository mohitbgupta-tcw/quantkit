import datetime
import quantkit.bt.frequency.core as core


class RunDaily(core.RunPeriod):
    """
    Algorithm to determine if day has changed.
    Useful for daily rebalancing strategies.

    Parameters
    ----------
    run_on_first_date: bool, optional
        run first time the Algo is called
    run_on_end_of_period: bool, optional
        run on end of period
    run_on_last_date: bool, optional
        run last time the Algo is called
    """

    def compare_dates(self, now: datetime.date, date_to_compare: datetime.date) -> bool:
        """
        Compare two dates and check if day has changed

        Parameters
        ----------
        now: datetime.date
            current date
        date_to_compare: datetime.date
            next (run on end of period) or previous (run on beginning of period) date

        Returns
        -------
        bool:
            day has changed
        """
        if now.date() != date_to_compare.date():
            return True
        return False


class RunWeekly(core.RunPeriod):
    """
    Algorithm to determine if week has changed.
    Useful for weekly rebalancing strategies.

    Parameters
    ----------
    run_on_first_date: bool, optional
        run first time the Algo is called
    run_on_end_of_period: bool, optional
        run on end of period
    run_on_last_date: bool, optional
        run last time the Algo is called
    """

    def compare_dates(self, now: datetime.date, date_to_compare: datetime.date) -> bool:
        """
        Compare two dates and check if week has changed

        Parameters
        ----------
        now: datetime.date
            current date
        date_to_compare: datetime.date
            next (run on end of period) or previous (run on beginning of period) date

        Returns
        -------
        bool:
            week has changed
        """
        if now.week != date_to_compare.week:
            return True
        return False


class RunMonthly(core.RunPeriod):
    """
    Algorithm to determine if month has changed.
    Useful for monthly rebalancing strategies.

    Parameters
    ----------
    run_on_first_date: bool, optional
        run first time the Algo is called
    run_on_end_of_period: bool, optional
        run on end of period
    run_on_last_date: bool, optional
        run last time the Algo is called
    """

    def compare_dates(self, now: datetime.date, date_to_compare: datetime.date) -> bool:
        """
        Compare two dates and check if month has changed

        Parameters
        ----------
        now: datetime.date
            current date
        date_to_compare: datetime.date
            next (run on end of period) or previous (run on beginning of period) date

        Returns
        -------
        bool:
            month has changed
        """
        if now.month != date_to_compare.month:
            return True
        return False


class RunQuarterly(core.RunPeriod):
    """
    Algorithm to determine if quarter has changed.
    Useful for quarterly rebalancing strategies.

    Parameters
    ----------
    run_on_first_date: bool, optional
        run first time the Algo is called
    run_on_end_of_period: bool, optional
        run on end of period
    run_on_last_date: bool, optional
        run last time the Algo is called
    """

    def compare_dates(self, now: datetime.date, date_to_compare: datetime.date) -> bool:
        """
        Compare two dates and check if quarter has changed

        Parameters
        ----------
        now: datetime.date
            current date
        date_to_compare: datetime.date
            next (run on end of period) or previous (run on beginning of period) date

        Returns
        -------
        bool:
            quarter has changed
        """
        if now.quarter != date_to_compare.quarter:
            return True
        return False


class RunYearly(core.RunPeriod):
    """
    Algorithm to determine if year has changed.
    Useful for yearly rebalancing strategies.

    Parameters
    ----------
    run_on_first_date: bool, optional
        run first time the Algo is called
    run_on_end_of_period: bool, optional
        run on end of period
    run_on_last_date: bool, optional
        run last time the Algo is called
    """

    def compare_dates(self, now: datetime.date, date_to_compare: datetime.date) -> bool:
        """
        Compare two dates and check if year has changed

        Parameters
        ----------
        now: datetime.date
            current date
        date_to_compare: datetime.date
            next (run on end of period) or previous (run on beginning of period) date

        Returns
        -------
        bool:
            year has changed
        """
        if now.year != date_to_compare.year:
            return True
        return False
