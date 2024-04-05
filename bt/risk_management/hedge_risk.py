import numpy as np
import quantkit.bt.core_structure.algo as algo


class ResolveOnTheRun(algo.Algo):
    """
    Looks at securities set in temp['selected'] and searches for names that
    match the names of "aliases" for on-the-run securities in the provided
    data. Then replaces the alias with the name of the underlying security
    appropriate for the given date, and sets it back on temp['selected']
    -> data has to be passed in during initializing of strategy

    Example
    -------
    govt_otr =
                    govt_10Y
    2020-01-01	govt_2029_12
    2020-01-02	govt_2029_12
    2020-01-03	govt_2029_12
    2020-01-06	govt_2029_12
    2020-01-07	govt_2029_12

    additional_data = {
        "govt_otr" : govt_otr
    }

    SelectThese(["govt_10Y"])
    ResolveOnTheRun("govt_otr")

    Parameters
    ----------
    on_the_run: str
        name of DataFrame with
            - columns set to "on the run" ticker names
            - index set to the timeline for the backtest
            - values are the actual security name to use for the given date
    include_negative: bool
        include securities that have negative or zero prices
    """

    def __init__(self, on_the_run: str, include_negative: bool = False) -> None:
        super().__init__()
        self.on_the_run = on_the_run
        self.include_negative = include_negative

    def __call__(self, target) -> bool:
        """
        Run Algo on call ResolveOnTheRun()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            resolve done
        """
        # Resolve real tickers based on OTR
        on_the_run = target.get_data(self.on_the_run)
        selected = target.temp["selected"]
        aliases = [s for s in selected if s in on_the_run.columns]
        resolved = on_the_run.loc[target.now, aliases].tolist()

        universe = target.universe.loc[target.now, resolved].dropna()
        if self.include_negative:
            resolved = list(universe.index)
        else:
            resolved = list(universe[universe > 0].index)
        target.temp["selected"] = resolved + [
            s for s in selected if s not in on_the_run.columns
        ]
        return True


class HedgeRisks(algo.Algo):
    """
    Hedges risk measures with selected instruments.
    -> make sure UpdateRisk Algo has been called beforehand

    Parameters
    ----------
    measures: list
        names of risk measures to hedge
    pseudo: bool
        use pseudo-inverse to compute inverse Jacobian
    """

    def __init__(self, measures, pseudo=False) -> None:
        super().__init__()
        if len(measures) == 0:
            raise ValueError("Must pass in at least one measure to hedge")
        self.measures = measures
        self.pseudo = pseudo

    def _get_target_risk(self, target, measure) -> float:
        """
        Get risk value of target strategy

        Parameters
        ----------
        target: Strategy
            strategy of backtest
        measure: str
            name of risk measure

        Returns
        -------
        float:
            risk value
        """
        if not hasattr(target, "risk"):
            raise ValueError("risk not set up on target %s" % target.name)
        if measure not in target.risk:
            raise ValueError("measure %s not set on target %s" % (measure, target.name))
        return target.risk[measure]

    def __call__(self, target) -> bool:
        """
        Run Algo on call HedgeRisks()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            risk update done
        """
        securities = target.temp["selected"]

        # Get target risk
        target_risk = np.array(
            [self._get_target_risk(target, m) for m in self.measures]
        )
        target_risk = target_risk.reshape(len(self.measures), 1)

        # Get hedge risk as a Jacobian matrix
        data = []
        for m in self.measures:
            d = target.get_data("unit_risk").get(m)
            if d is None:
                raise ValueError(
                    "unit_risk for %s not present in temp on %s"
                    % (self.measure, target.name)
                )
            data.append((target.inow, d))

        hedge_risk = np.array([[d[s].values[i] for (i, d) in data] for s in securities])

        # Get hedge ratios
        if self.pseudo:
            inv = np.linalg.pinv(hedge_risk).T
        else:
            inv = np.linalg.inv(hedge_risk).T
        notionals = np.matmul(inv, -target_risk).flatten()

        # Hedge
        for notional, security in zip(notionals, securities):
            if np.isnan(notional):
                raise ValueError("%s has nan hedge notional" % security)
            target.transact(notional, security)
        return True
