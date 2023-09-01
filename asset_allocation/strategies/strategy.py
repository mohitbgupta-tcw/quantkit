import quantkit.asset_allocation.return_calc.log_return as log_return
import quantkit.asset_allocation.return_calc.ewma_return as ewma_return
import quantkit.asset_allocation.return_calc.simple_return as simple_return
import quantkit.asset_allocation.return_calc.cumprod_return as cumprod_return
import quantkit.asset_allocation.risk_calc.log_vol as log_vol
import quantkit.asset_allocation.risk_calc.ewma_vol as ewma_vol
import quantkit.asset_allocation.risk_calc.simple_vol as simple_vol
import quantkit.asset_allocation.allocation.mean_variance as mean_variance
import quantkit.asset_allocation.allocation.min_variance as min_variance
import quantkit.asset_allocation.allocation.risk_parity as risk_parity
import quantkit.asset_allocation.allocation.equal_weight as equal_weight
import pandas as pd
import numpy as np
import datetime
from typing import Tuple, Dict, Union, List


class Strategy(object):
    def __init__(
        self,
        universe,
        return_engine,
        risk_engine,
        frequency,
        rebalance_dates,
        trans_cost,
        allocation_models,
        weight_constraint,
        **kwargs,
    ):
        risk_return_engine_kwargs = dict(
            frequency=frequency, ddof=1, geo_base=1, adjust=True, half_life=12, span=36
        )
        self.rebalance_dates = rebalance_dates
        self.all_portfolios = pd.DataFrame(columns=["portfolio_name", "return"])
        self.universe = universe
        self.num_total_assets = len(universe)
        self.trans_cost = np.ones(self.num_total_assets) * trans_cost

        # return engine
        if return_engine == "log_normal":
            self.return_engine = log_return.LogReturn(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "ewma":
            self.return_engine = ewma_return.LogEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "ewma_rolling":
            self.return_engine = ewma_return.RollingLogEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "simple":
            self.return_engine = simple_return.SimpleExp(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "simple":
            self.return_engine = simple_return.SimpleExp(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "cumprod":
            self.return_engine = cumprod_return.CumProdReturn(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        else:
            return_engine = None

        # risk engine
        if risk_engine == "log_normal":
            self.risk_engine = log_vol.LogNormalVol(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif risk_engine == "ewma":
            self.risk_engine = ewma_vol.LogNormalEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif risk_engine == "ewma_rolling":
            self.risk_engine = ewma_vol.RollingLogNormalEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif risk_engine == "simple":
            self.risk_engine = simple_vol.SimpleVol(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        else:
            risk_engine = None

        # Allocation Engine
        allocation_engine_kwargs = dict(
            asset_list=universe,
            risk_engine=self.risk_engine,
            return_engine=self.return_engine,
        )
        self.allocation_engines_d = dict()

        for allocation_model in allocation_models:
            if allocation_model == "mean_variance":
                this_allocation_engine = mean_variance.MeanVariance(
                    **allocation_engine_kwargs
                )

            elif allocation_model == "constrained_mean_variance":
                allocation_engine_kwargs.update(
                    dict(
                        weights_constraint=self.get_weights_constraints_d(
                            weight_constraint
                        )
                    )
                )
                this_allocation_engine = mean_variance.MeanVariance(
                    **allocation_engine_kwargs
                )

            elif allocation_model == "min_variance":
                this_allocation_engine = min_variance.MinimumVariance(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "constrained_min_variance":
                wc = self.get_weights_constraints_d(weight_constraint)
                print(wc)
                print(weight_constraint)
                this_allocation_engine = min_variance.MinimumVariance(
                    weight_constraint=wc, **allocation_engine_kwargs
                )
            elif allocation_model == "risk_parity":
                this_allocation_engine = risk_parity.RiskParity(
                    **allocation_engine_kwargs
                )

            elif allocation_model == "equal_weight":
                this_allocation_engine = equal_weight.EqualWeight(
                    **allocation_engine_kwargs
                )

            else:
                raise RuntimeError(
                    f"allocation_model { allocation_model } is not defined.."
                )

            self.allocation_engines_d[allocation_model] = this_allocation_engine

    def assign(
        self,
        date,
        price_return,
        annualize_factor=1.0,
    ) -> None:
        """
        Transform and assign returns to the actual calculator
        Parameter
        ---------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency

        Return
        ------
        """
        self.return_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )
        self.risk_engine.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )

    def get_risk_budgets(self, date: datetime.date):
        """
        get risk budgets for each allocation in allocation_engines_d
        allocation_engines_d contains each allocation defined in allocation_models in the config file
        risk budgets are needed in risk parity strategy, for all other strategies not relevant and defaulted to None

        Parameters
        ----------
        date : datetime.date
            date for recorded budgets

        Returns
        -------
        dict[allocation: budgets]
            dictionary which maps risk budgets per asset as np.array to allocations
        """
        budgets = dict(
            [
                (allocation_name, getattr(allocation_engine, "risk_budgets", None))
                for allocation_name, allocation_engine in self.allocation_engines_d.items()
            ]
        )
        return budgets

    def calculate_allocations(self, date):
        if not date in self.rebalance_dates:
            return
        if date < self.rebalance_dates[40]:
            return

        risk_budgets = self.get_risk_budgets(date)
        for allocation_name, allocation_engine in self.allocation_engines_d.items():
            this_risk_budget = risk_budgets.get(allocation_name)
            allocation_engine.update(
                risk_budgets=this_risk_budget, selected_assets=self.selected_securities
            )
            allocation_engine.allocate(date, self.selected_securities)
        return

    def get_weights_constraints_d(self, weight_constraint) -> Dict[str, List[float]]:
        """
        Initialize weight constraints for constrained_mean_variance:
        first checks for constraint on asset level, if not provided defaults to default weight constraint
        weights are set in asset_weights_constraint and default_weights_constraint in config file

        Parameters
        ----------
        params : dict
            Configuration file of constrained mean variance setting as dictionary

        Returns
        -------
        Dict[str, List[float]]
            Dictionary which indicates weight range for each asset provided
        """
        w_consts_d = dict()
        for this_id in self.universe:
            w_consts_d[this_id] = weight_constraint
        return w_consts_d

    @property
    def return_metrics_intuitive(self):
        """
        Forecaseted returns from return engine

        Parameter
        ---------

        Return
        ------
        <np.array>
            returns
        """
        return self.return_engine.return_metrics_optimizer

    @property
    def selected_securities(self) -> np.array:
        """
        Index of top n momentum returns

        Parameter
        ---------

        Return
        ------
        <np.array>
            index
        """
        raise NotImplementedError()
