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
import quantkit.asset_allocation.allocation.market_weight as market_weight
import quantkit.utils.mapping_configs as mapping_configs
import pandas as pd
import numpy as np
import datetime


class Strategy(object):
    """
    Base class for trading strategies

    Parameters
    ----------
    universe: list
        investment universe
    return_engine: mstar_asset_allocation.return_calc.return_metrics
        return engine used to forecast returns
    risk_engine: mstar_asset_allocation.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    frequency: str
        frequency of return data
    rebelance: str
        rebalance frequency
    rebalance_dates: list
        list of rebalancing dates
    trans_cost: float
        transaction cost
    allocation_models: list
        list of weighting strategies
    weight_constraint: list
        list of lower and upper bound for weight constraints
    """

    def __init__(
        self,
        universe: list,
        return_engine,
        risk_engine,
        frequency: str,
        rebalance: str,
        rebalance_dates: list,
        trans_cost: float,
        allocation_models: list,
        weight_constraint: list,
        **kwargs,
    ) -> None:
        risk_return_engine_kwargs = dict(
            frequency=frequency, ddof=1, geo_base=1, adjust=True, half_life=12, span=36
        )
        self.waiting_period = mapping_configs.annualize_factor_d[rebalance]
        self.rebalance = rebalance
        self.rebalance_dates = rebalance_dates
        self.all_portfolios = pd.DataFrame(columns=["portfolio_name", "return"])
        self.universe = universe
        self.num_total_assets = len(universe)
        self.trans_cost = np.ones(self.num_total_assets) * trans_cost
        self.kwargs = kwargs

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

        wc = self.get_weights_constraints_d(weight_constraint)
        for allocation_model in allocation_models:
            if allocation_model == "mean_variance":
                this_allocation_engine = mean_variance.MeanVariance(
                    **allocation_engine_kwargs
                )

            elif allocation_model == "constrained_mean_variance":
                this_allocation_engine = mean_variance.MeanVariance(
                    weights_constraint=wc, **allocation_engine_kwargs
                )

            elif allocation_model == "min_variance":
                this_allocation_engine = min_variance.MinimumVariance(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "constrained_min_variance":
                this_allocation_engine = min_variance.MinimumVariance(
                    weights_constraint=wc, **allocation_engine_kwargs
                )
            elif allocation_model == "risk_parity":
                this_allocation_engine = risk_parity.RiskParity(
                    **allocation_engine_kwargs
                )

            elif allocation_model == "equal_weight":
                this_allocation_engine = equal_weight.EqualWeight(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "market_weight":
                this_allocation_engine = market_weight.MarketWeight(
                    **allocation_engine_kwargs
                )
            else:
                raise RuntimeError(
                    f"allocation_model { allocation_model } is not defined.."
                )

            self.allocation_engines_d[allocation_model] = this_allocation_engine

        # portfolio engine
        self.portfolio_risk_return_engine_kwargs = dict(
            frequency=frequency,
            ddof=1,
            geo_base=1,
        )
        # portfolio engine
        self.portfolio_risk_engine = simple_vol.SimpleVol(
            universe=self.universe,
            **self.portfolio_risk_return_engine_kwargs,
            **self.kwargs,
        )
        self.portfolio_return_engine = simple_return.SimpleExp(
            universe=self.universe,
            **self.portfolio_risk_return_engine_kwargs,
            **self.kwargs,
        )

    def assign(
        self,
        date: datetime.date,
        price_return: np.array,
        index_comp: np.array,
        annualize_factor: int = 1.0,
        **kwargs,
    ) -> None:
        """
        Transform and assign returns to the actual calculator

        Parameters
        ----------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        index_comp: np.array
            index components for date
        annualize_factor: int, optional
            factor depending on data frequency
        """
        self.latest_return = price_return
        self.index_comp = index_comp

    def get_risk_budgets(self, date: datetime.date) -> dict:
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
        dict
            dictionary which maps risk budgets per asset as np.array to allocations
        """
        budgets = dict(
            [
                (allocation_name, getattr(allocation_engine, "risk_budgets", None))
                for allocation_name, allocation_engine in self.allocation_engines_d.items()
            ]
        )
        return budgets

    def get_portfolio_stats(
        self,
        allocation: np.array,
        next_allocation: np.array,
    ) -> pd.DataFrame:
        """
        Portfolio level stats

        Parameters
        ----------
        allocation: np.array
            weight allocation in the order of universe_tickers
        next_allocation: np.array
            weight allocation of next period in the order of universe_tickers

        Returns
        -------
        pd.DataFrame
            DataFrame with risk and returns set in frequency over last rebalance period
        """
        portfolio_return = self.portfolio_return_engine.get_portfolio_return(
            allocation,
            next_allocation=next_allocation,
            trans_cost=self.trans_cost,
        )
        return portfolio_return

    def get_allocation(self, date: datetime.date, allocation_model: str):
        """
        Get allocation and next for allocation model by accessing allocation history

        Parameters
        ----------
        date : datetime.date
            date of snapshot
        allocation_model: str
            name of allocation model as set in allocation_engines_d

        Returns
        -------
        np.array
            last allocation in the order of universe_tickers
        np.array
            allocation for snapshot date in the order of universe_tickers
        """

        allocations = self.allocation_engines_d.get(
            allocation_model
        ).allocations_history
        allocation_pd = pd.DataFrame.from_dict(allocations, orient="index")
        portfolio_allocation = allocation_pd.shift(1).loc[date].values
        next_portfolio_allocation = allocation_pd.loc[date].values
        return portfolio_allocation, next_portfolio_allocation

    def backtest(self, date: datetime.date, market_caps: np.array) -> None:
        """
        - Calculate optimal allocation for each weighting strategy
        - Calculate allocation returns

        Parameters
        ----------
        date: datetime.date
            snapshot date
        market_caps: np.array
            market caps of assets in universe
        waiting_period: int, optional
            waiting period before first covariance gets calculated
        """
        if not date in self.rebalance_dates:
            return

        # need enough data points for cov to be calculated
        if date < self.rebalance_dates[self.waiting_period]:
            self.portfolio_risk_engine = simple_vol.SimpleVol(
                universe=self.universe,
                **self.portfolio_risk_return_engine_kwargs,
                **self.kwargs,
            )
            self.portfolio_return_engine = simple_return.SimpleExp(
                universe=self.universe,
                **self.portfolio_risk_return_engine_kwargs,
                **self.kwargs,
            )
            return

        risk_budgets = self.get_risk_budgets(date)
        for allocation_name, allocation_engine in self.allocation_engines_d.items():
            this_risk_budget = risk_budgets.get(allocation_name)
            allocation_engine.update(
                selected_assets=self.selected_securities,
                risk_budgets=this_risk_budget,
                market_caps=market_caps,
            )
            allocation_engine.allocate(date, self.selected_securities)

        for allocation_model in self.allocation_engines_d:
            ex_ante_allocation, ex_post_allocation = self.get_allocation(
                date, allocation_model
            )

            ex_ante_portfolio_return = self.get_portfolio_stats(
                ex_ante_allocation, ex_post_allocation
            )
            ex_ante_portfolio_return["portfolio_name"] = f"ex_ante_{allocation_model}"
            self.all_portfolios = pd.concat(
                [self.all_portfolios, ex_ante_portfolio_return], axis=0
            )

        # portfolio engine
        self.portfolio_risk_engine = simple_vol.SimpleVol(
            universe=self.universe,
            **self.portfolio_risk_return_engine_kwargs,
            **self.kwargs,
        )
        self.portfolio_return_engine = simple_return.SimpleExp(
            universe=self.universe,
            **self.portfolio_risk_return_engine_kwargs,
            **self.kwargs,
        )

    def get_weights_constraints_d(self, weight_constraint: list) -> dict:
        """
        Initialize weight constraints for constrained strategies
        weights are set in default_weights_constraint in config file

        Parameters
        ----------
        weight_constraint : list
            list of lower and upper bound for weight constraints

        Returns
        -------
        dict
            Dictionary which indicates weight range for each asset provided
        """
        w_consts_d = dict()
        for this_id in self.universe:
            w_consts_d[this_id] = weight_constraint
        return w_consts_d

    @property
    def return_metrics_intuitive(self) -> np.array:
        """
        All forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_engine.return_metrics_optimizer

    @property
    def selected_securities(self) -> np.array:
        """
        Index (position in universe_tickers as integer) of all selected securities by that strategy


        Returns
        -------
        np.array
            array of indexes
        """
        raise NotImplementedError()
