import quantkit.asset_allocation.allocation.mean_variance as mean_variance
import quantkit.asset_allocation.allocation.min_variance as min_variance
import quantkit.asset_allocation.allocation.risk_parity as risk_parity
import quantkit.asset_allocation.allocation.hrp as hrp
import quantkit.asset_allocation.allocation.equal_weight as equal_weight
import quantkit.asset_allocation.allocation.market_weight as market_weight
import quantkit.asset_allocation.allocation.original_weight as original_weight
import quantkit.asset_allocation.risk_management.stop_loss.buy_to_low as buy_to_low
import quantkit.asset_allocation.risk_management.stop_loss.high_to_low as high_to_low
import quantkit.asset_allocation.risk_management.stop_loss.no_stop as no_stop
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
    return_engine: asset_allocation.return_calc.return_metrics
        return engine used to forecast returns
    risk_engine: asset_allocation.risk_calc.risk_metrics
        risk engine used to forecast cov matrix
    portfolio_return_engine: asset_allocation.return_calc.return_metrics
        portfolio return engine used to forecast returns
    stop_loss: str
        Stop-Loss strategy, if none set to None
    stop_loss_threshold: float,
        Stop-Loss threshold percentage
    frequency: str
        frequency of return data
    rebelance: str
        rebalance frequency
    trans_cost: float
        transaction cost
    allocation_models: list
        list of weighting strategies
    weight_constraint: dict
        list of lower and upper bound for each asset
    portfolio_leverage: float
        portfolio leverage
    scaling: dict
        dictionary to scale assets, must have the following components:
        {
            "limited_assets": [],
            "limit": 0.35,
            "allocate_to": []
        }
    """

    def __init__(
        self,
        universe: list,
        return_engine,
        risk_engine,
        portfolio_return_engine,
        stop_loss: str,
        stop_loss_threshold: float,
        frequency: str,
        rebalance: str,
        trans_cost: float,
        allocation_models: list,
        weight_constraint: dict,
        portfolio_leverage: float,
        scaling: dict,
        **kwargs,
    ) -> None:
        self.rebalance = rebalance
        self.all_portfolios = pd.DataFrame(columns=["portfolio_name", "return"])
        self.universe = universe
        self.num_total_assets = len(universe)
        self.trans_cost = np.ones(self.num_total_assets) * trans_cost
        self.portfolio_leverage = portfolio_leverage
        self.return_engine = return_engine
        self.risk_engine = risk_engine
        self.portfolio_return_engine = portfolio_return_engine

        # Allocation Engine
        allocation_engine_kwargs = dict(
            asset_list=universe,
            risk_engine=self.risk_engine,
            return_engine=self.return_engine,
            portfolio_leverage=portfolio_leverage,
        )
        self.allocation_engines_d = dict()

        for allocation_model in allocation_models:
            if allocation_model == "mean_variance":
                this_allocation_engine = mean_variance.MeanVariance(
                    **allocation_engine_kwargs
                )

            elif allocation_model == "constrained_mean_variance":
                this_allocation_engine = mean_variance.MeanVariance(
                    weights_constraint=weight_constraint, **allocation_engine_kwargs
                )

            elif allocation_model == "min_variance":
                this_allocation_engine = min_variance.MinimumVariance(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "constrained_min_variance":
                this_allocation_engine = min_variance.MinimumVariance(
                    weights_constraint=weight_constraint, **allocation_engine_kwargs
                )
            elif allocation_model == "risk_parity":
                this_allocation_engine = risk_parity.RiskParity(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "hrp":
                this_allocation_engine = hrp.HierarchicalRiskParity(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "constrained_hrp":
                this_allocation_engine = hrp.HierarchicalRiskParity(
                    weights_constraint=weight_constraint, **allocation_engine_kwargs
                )
            elif allocation_model == "scaled_hrp":
                this_allocation_engine = hrp.HierarchicalRiskParity(
                    weights_constraint=weight_constraint,
                    scaling=scaling,
                    **allocation_engine_kwargs,
                )
            elif allocation_model == "equal_weight":
                this_allocation_engine = equal_weight.EqualWeight(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "market_weight":
                this_allocation_engine = market_weight.MarketWeight(
                    **allocation_engine_kwargs
                )
            elif allocation_model == "original_weight":
                this_allocation_engine = original_weight.OriginalWeight(
                    **allocation_engine_kwargs
                )
            else:
                raise RuntimeError(
                    f"allocation_model { allocation_model } is not defined.."
                )

            self.allocation_engines_d[allocation_model] = this_allocation_engine

        # stop-loss
        if stop_loss == "high_low":
            self.stop_loss = high_to_low.HighToLow(
                universe=self.universe,
                stop_threshold=stop_loss_threshold,
                frequency=frequency,
                rebalance=rebalance,
            )
        elif stop_loss == "buy_low":
            self.stop_loss = buy_to_low.BuyToLow(
                universe=self.universe,
                stop_threshold=stop_loss_threshold,
                frequency=frequency,
                rebalance=rebalance,
            )
        else:
            self.stop_loss = no_stop.NoStop(
                universe=self.universe,
                stop_threshold=stop_loss_threshold,
                frequency=frequency,
                rebalance=rebalance,
            )

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        index_comp: np.ndarray,
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
        self.stop_loss.assign(
            date=date, price_return=price_return, annualize_factor=annualize_factor
        )

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
        allocation: np.ndarray,
        next_allocation: np.ndarray,
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
            stopped_securities_matrix=np.array(
                self.stop_loss.stopped_securities_matrix
            ),
            next_allocation=next_allocation,
            trans_cost=self.trans_cost,
            leverage=self.portfolio_leverage,
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

    def backtest(
        self,
        date: datetime.date,
        index_comp: np.ndarray,
        market_caps: np.ndarray,
        fama_french_factors: np.ndarray,
        **kwargs,
    ) -> None:
        """
        - Calculate optimal allocation for each weighting strategy
        - Calculate allocation returns

        Parameters
        ----------
        date: datetime.date
            snapshot date
        index_comp: np.array
            index components for date
        market_caps: np.array
            market caps of assets in universe
        fama_french_factors: np.array
            fama french factors for regression
        """
        risk_budgets = self.get_risk_budgets(date)
        for allocation_name, allocation_engine in self.allocation_engines_d.items():
            this_risk_budget = risk_budgets.get(allocation_name)
            allocation_engine.update(
                selected_assets=self.selected_securities,
                risk_budgets=this_risk_budget,
                market_caps=market_caps,
                weights=index_comp,
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
            cum_return = ((ex_ante_portfolio_return["return"] + 1).cumprod() - 1)[
                -1:
            ].to_numpy()
            self.allocation_engines_d[allocation_model].run_factor_regression(
                fama_french_factors, cum_return, date
            )

    @property
    def return_metrics_intuitive(self) -> np.ndarray:
        """
        All forecaseted returns from return engine

        Returns
        -------
        np.array
            returns
        """
        return self.return_engine.return_metrics_intuitive

    @property
    def selected_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of all selected securities by that strategy


        Returns
        -------
        np.array
            array of indexes
        """
        raise NotImplementedError()

    def is_valid(self):
        """
        check if inputs are valid

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return self.return_engine.is_valid() and self.risk_engine.is_valid()
