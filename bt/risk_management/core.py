import numpy as np
import pandas as pd
import quantkit.bt.core_structure.algo as algo
import quantkit.bt.util.util_functions as util_functions


class UpdateRisk(algo.Algo):
    """
    Track risk measure on all nodes of strategy.
    -> add unit_risk to additional_data when initializing backtest
    -> should come in same frequency as prices, on security level
    -> set "risk" attribute on target and all its children

    Example
    -------
    beta =
                corp0000	    corp0001	corp0002
    2020-01-01	-1.122290e-04	-0.000399	-6.993390e-05
    2020-01-02	-1.118618e-04	-0.000400	-6.973931e-05
    2020-01-03	-1.113191e-04	-0.000399	-6.943006e-05

    additional_data = {
        'unit_risk' : {
            'pvbp' : pd.concat( [ govt_pvbp, corp_pvbp], axis=1)/100.,
            'beta' : beta
            }
        }

    risk_core.UpdateRisk("pvbp", history=1)
    risk_core.UpdateRisk("beta", history=1)

    Parameters
    ----------
    measure: str
        the name of the risk measure (pvbp, beta)
    """

    def __init__(self, measure: str) -> None:
        super().__init__(name=f"UpdateRisk>{measure}")
        self.measure = measure

    def _setup_measure(self, target) -> None:
        """
        Setup risk measure within the risk attributes of node in question

        Parameters
        ----------
        target: Strategy
            strategy of backtest
        """
        if not hasattr(target, "risk"):
            target.risk = {}
            target.risks = pd.DataFrame(index=target.data.index)
        if self.measure not in target.risk:
            target.risk[self.measure] = np.nan
            target.risks[self.measure] = np.nan

    def _set_risk_recursive(self, target, unit_risk_frame: pd.DataFrame) -> None:
        """
        Update risk measurement for current period

        Calculation
        -----------
        Security: position size * unit risk
        Portfolio: \sum{security risks}

        Parameters
        ----------
        target: Strategy
            strategy of backtest
        unit_risk_frame: pd.DataFrame
            DataFrame of security risk
        """
        # General setup of risk on nodes
        self._setup_measure(target)

        if target._issec:
            unit_risk = unit_risk_frame[target.name].values[target.inow]
            if util_functions.is_zero(target.position):
                risk = 0.0
            else:
                risk = unit_risk * target.position * target.multiplier
        else:
            risk = 0.0
            for child in target.children.values():
                self._set_risk_recursive(child, unit_risk_frame)
                risk += child.risk[self.measure]

        target.risk[self.measure] = risk
        target.risks.loc[target.now, self.measure] = risk

    def __call__(self, target) -> bool:
        """
        Run Algo on call UpdateRisk()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            risk update done
        """
        unit_risk_frame = target.get_data("unit_risk")[self.measure]
        self._set_risk_recursive(target, unit_risk_frame)
        return True
