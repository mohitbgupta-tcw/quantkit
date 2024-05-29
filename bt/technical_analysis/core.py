import quantkit.bt.core_structure.algo as algo


class SetStat(algo.Algo):
    """
    Calculate statistics used in downstream Algos
    -> save them in temp["stat"]
    -> stat should be submitted in initial setup of strategy and have same index as universe

    Example
    -------
    additional_data = {
        'coupons' : pd.concat([govt_accrued, corp_accrued], axis=1) / 100.,
        'govt_maturity' : govt_data,
        'corp_maturity' : corp_data,
        'govt_roll_map' : govt_roll_map,
        'govt_otr' : govt_otr,
        'corp_yield' : corp_yield,
    }
    SetStat('corp_yield')

    Parameters
    ----------
    stat: str
        name of stat
    """

    def __init__(self, stat: str) -> None:
        super().__init__()
        self.stat_name = stat

    def __call__(self, target) -> bool:
        """
        Run Algo on call SetStat()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        stat = target.get_data(self.stat_name)
        target.temp["stat"] = stat.loc[target.now]
        return True
