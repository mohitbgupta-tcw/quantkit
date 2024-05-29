import quantkit.bt.core_structure.algo as algo


class ClosePositionsAfterDates(algo.Algo):
    """
    Close positions on securities after a given date.
    -> pass close map into additional_data when initializing backtest
    -> sets target.perm["closed"]
    -> keep track of which securities have already been closed

    Example
    -------
    maturity =
                    date
    govt_2029_12	2029-12-31
    govt_2030_03	2030-03-31
    govt_2030_06	2030-06-30
    govt_2030_09	2030-09-30
    govt_2030_12	2030-12-31
    govt_2031_03	2031-03-31
    govt_2031_06	2031-06-30
    govt_2031_09	2031-09-30
    govt_2031_12	2031-12-31

    additional_data = {
        "maturity" : maturity,
        }

    ClosePositionsAfterDates("maturity")

    Parameters
    ----------
    close_dates: str
        name of DataFrame indexed by security name, with columns
            - "date": the date after which we want to close the position
    """

    def __init__(self, close_dates: str) -> None:
        super().__init__()
        self.run_always = True
        self.close_dates = close_dates

    def __call__(self, target) -> bool:
        """
        Run Algo on call ClosePositionsAfterDates()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "closed" not in target.perm:
            target.perm["closed"] = set()
        close_dates = target.get_data(self.close_dates)["date"]
        # Find securities that are candidate for closing
        sec_names = [
            sec_name
            for sec_name, sec in target.children.items()
            if sec._issec
            and sec_name in close_dates.index
            and sec_name not in target.perm["closed"]
        ]

        # Check whether closed
        is_closed = close_dates.loc[sec_names] <= target.now

        # Close position
        for sec_name in is_closed[is_closed].index:
            target.close(sec_name)
            target.perm["closed"].add(sec_name)
        return True
