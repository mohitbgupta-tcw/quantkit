import pandas as pd
import numpy as np
from copy import deepcopy
import quantkit.runner as loader
import quantkit.utils.logging as logging
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.backtester.strategies.momentum as momentum
import quantkit.backtester.strategies.mean_reversion as mean_reversion
import quantkit.backtester.strategies.pick_all as pick_all
import quantkit.backtester.strategies.relative_value as relative_value
import quantkit.backtester.strategies.magic_formula as magic_formula
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.backtester.data_loader.portfolio_datasource as portfolio_datasource
import quantkit.backtester.data_loader.parent_issuer_datasource as parent_issuer_datasource
import quantkit.backtester.data_loader.prices_datasource as prices_datasource
import quantkit.backtester.data_loader.fundamentals_datasource as fundamentals_datasource
import quantkit.backtester.data_loader.marketmultiple_datasource as marketmultiple_datasource
import quantkit.backtester.return_calc.log_return as log_return
import quantkit.backtester.return_calc.ewma_return as ewma_return
import quantkit.backtester.return_calc.simple_return as simple_return
import quantkit.backtester.return_calc.cumprod_return as cumprod_return
import quantkit.backtester.return_calc.cumprod_return as cumprod_return
import quantkit.backtester.risk_calc.log_vol as log_vol
import quantkit.backtester.risk_calc.ewma_vol as ewma_vol
import quantkit.backtester.risk_calc.simple_vol as simple_vol
import sys


class Runner(loader.Runner):
    def init(self, local_configs: str = ""):
        """
        - initialize datsources and load data
        - create reusable attributes
        - itereate over DataFrames and create connected objects

        Parameters
        ----------
        local_configs: str, optional
            path to a local configarations file
        """
        super().init(local_configs, "backtester")
        logging.log(sys.path)
        logging.log(self.params)
        logging.log('self.params******************************************')
        self.annualize_factor = mapping_configs.annualize_factor_d.get(
            self.params["prices_datasource"]["frequency"], 252
        )
        self.rebalance_window = mapping_configs.rebalance_window_d.get(
            self.params["prices_datasource"]["rebalance"]
        )

        self.strategies = dict()
        self.return_engines = dict()
        self.risk_engines = dict()

        # connect portfolio datasource
        self.portfolio_datasource = portfolio_datasource.PortfolioDataSource(
            params=self.params["portfolio_datasource"], api_settings=self.api_settings
        )

        # connect parent issuer datasource
        self.parent_issuer_datasource = (
            parent_issuer_datasource.TickerParentIssuerSource(
                params=self.params["ticker_parent_issuer_datasource"],
                api_settings=self.api_settings,
            )
        )

        # connect fundamental and price datasource
        self.fundamentals_datasource = fundamentals_datasource.FundamentalsDataSource(
            params=self.params["fundamentals_datasource"],
            api_settings=self.api_settings,
        )
        self.prices_datasource = prices_datasource.PricesDataSource(
            params=self.params["prices_datasource"],
            api_settings=self.api_settings,
        )
        self.marketmultiple_datasource = (
            marketmultiple_datasource.MarketMultipleDataSource(
                params=self.params["marketmultiple_datasource"],
                api_settings=self.api_settings,
            )
        )

        # iterate over dataframes and create objects
        logging.log("Start Iterating")
        self.iter()

    def iter(self) -> None:
        """
        iterate over DataFrames and create connected objects
        """
        #self.iter_parent_issuers()
        self.iter_portfolios()
        #self.iter_msci()
        #self.iter_prices()
        #self.iter_fundamentals()
        #self.iter_marketmuliples()
        #self.iter_holdings()
        #self.iter_securities()
        #self.iter_cash()
        #self.iter_companies()
        #self.iter_sovereigns()
        #self.iter_securitized()
        #self.iter_muni()
        #self.init_strategies()

    def iter_parent_issuers(self) -> None:
        """
        iterate over parent issuers
        """
        self.parent_issuer_datasource.load()
        self.parent_issuer_datasource.iter()

    def iter_fundamentals(self) -> None:
        """
        iterate over fundamental data
        """
        # load parent issuer data
        parent_tickers = self.parent_issuer_datasource.tickers()
        tickers = deepcopy(self.portfolio_datasource.all_tickers)
        tickers += parent_tickers
        tickers = list(set(tickers))

        # load fundamental data
        self.params["fundamentals_datasource"]["filters"]["ticker"] = tickers
        self.params["fundamentals_datasource"][
            "overwrite_parent"
        ] = self.parent_issuer_datasource.parent_issuers
        self.fundamentals_datasource.load(
            **self.params["fundamentals_datasource"]["filters"]["calendardate"]
        )
        self.fundamentals_datasource.iter(self.portfolio_datasource.all_tickers)

    def iter_prices(self) -> None:
        """
        iterate over price data
        """
        self.params["prices_datasource"]["filters"][
            "ticker"
        ] = self.portfolio_datasource.all_tickers
        self.prices_datasource.load(
            **self.params["prices_datasource"]["filters"]["date"]
        )
        self.prices_datasource.iter(self.portfolio_datasource.all_tickers)

    def iter_securities(self) -> None:
        """
        - Attach Fundamental information
        - Attach Price information
        """
        logging.log("Iterate Securities")
        for sec, sec_store in self.portfolio_datasource.securities.items():
            sec_store.iter(
                dict_fundamental=self.fundamentals_datasource.tickers,
                dict_prices=self.prices_datasource.tickers,
            )

    def iter_marketmuliples(self) -> None:
        """
        iterate over market multiple data
        """
        self.marketmultiple_datasource.load()
        self.marketmultiple_datasource.iter()

    def init_strategies(self) -> None:
        """
        Initialize all strategies defined in params file
        Each strategy should have at least the following parameters:
            - type: str
                strategy name
            - return_engine: str
                return calculator
            - risk_engine: str
                risk calculator
            - allocation_models: list
                list of all weighting strategies
        """
        # portfolio engine
        self.portfolio_risk_return_engine_kwargs = dict(
            frequency=self.params["prices_datasource"]["frequency"],
            ddof=1,
            geo_base=1,
            window_size=63,
        )
        # portfolio engines
        self.portfolio_risk_engine = simple_vol.SimpleVol(
            universe=self.portfolio_datasource.all_tickers,
            **self.portfolio_risk_return_engine_kwargs,
        )
        self.portfolio_return_engine = simple_return.SimpleExp(
            universe=self.portfolio_datasource.all_tickers,
            **self.portfolio_risk_return_engine_kwargs,
        )

        weight_constraint = self.get_weights_constraints_d()

        for strategy, strat_params in self.params["strategies"].items():
            risk_return_engine_kwargs = dict(
                frequency=self.params["prices_datasource"]["frequency"],
                ddof=1,
                geo_base=1,
                adjust=True,
                half_life=12,
                span=36,
            )
            # return engine
            return_engine = strat_params["return_engine"]
            window_size = (
                strat_params["return_window_size"]
                if "return_window_size" in strat_params
                else strat_params["window_size"]
            )
            if return_engine == "log_normal":
                return_engine = f"log_normal_{window_size}"
                self.return_engines[return_engine] = self.return_engines.get(
                    return_engine,
                    log_return.LogReturn(
                        universe=self.portfolio_datasource.all_tickers,
                        window_size=window_size,
                        **risk_return_engine_kwargs,
                    ),
                )
            elif return_engine == "ewma":
                self.return_engines[return_engine] = self.return_engines.get(
                    return_engine,
                    ewma_return.LogEWMA(
                        universe=self.portfolio_datasource.all_tickers,
                        **risk_return_engine_kwargs,
                    ),
                )
            elif return_engine == "ewma_rolling":
                self.return_engines[return_engine] = self.return_engines.get(
                    return_engine,
                    ewma_return.RollingLogEWMA(
                        universe=self.portfolio_datasource.all_tickers,
                        **risk_return_engine_kwargs,
                    ),
                )
            elif return_engine == "simple":
                return_engine = f"simple_{window_size}"
                self.return_engines[return_engine] = self.return_engines.get(
                    return_engine,
                    simple_return.SimpleExp(
                        universe=self.portfolio_datasource.all_tickers,
                        window_size=window_size,
                        **risk_return_engine_kwargs,
                    ),
                )
            elif return_engine == "cumprod":
                return_engine = f"cumprod_{window_size}"
                self.return_engines[return_engine] = self.return_engines.get(
                    return_engine,
                    cumprod_return.CumProdReturn(
                        universe=self.portfolio_datasource.all_tickers,
                        window_size=window_size,
                        **risk_return_engine_kwargs,
                    ),
                )
            strat_params["return_engine"] = self.return_engines[return_engine]

            # risk engine
            risk_engine = strat_params["risk_engine"]
            window_size = (
                strat_params["risk_window_size"]
                if "risk_window_size" in strat_params
                else strat_params["window_size"]
            )
            if risk_engine == "log_normal":
                risk_engine = f"log_normal_{window_size}"
                self.risk_engines[risk_engine] = self.risk_engines.get(
                    risk_engine,
                    log_vol.WindowLogNormalVol(
                        universe=self.portfolio_datasource.all_tickers,
                        window_size=window_size,
                        **risk_return_engine_kwargs,
                    ),
                )
            elif risk_engine == "ewma":
                self.risk_engines[risk_engine] = self.risk_engines.get(
                    risk_engine,
                    ewma_vol.LogNormalEWMA(
                        universe=self.portfolio_datasource.all_tickers,
                        **risk_return_engine_kwargs,
                    ),
                )
            elif risk_engine == "ewma_rolling":
                self.risk_engines[risk_engine] = self.risk_engines.get(
                    risk_engine,
                    ewma_vol.RollingLogNormalEWMA(
                        universe=self.portfolio_datasource.all_tickers,
                        **risk_return_engine_kwargs,
                    ),
                )
            elif risk_engine == "simple":
                risk_engine = f"simple_{window_size}"
                self.risk_engines[risk_engine] = self.risk_engines.get(
                    risk_engine,
                    simple_vol.SimpleVol(
                        universe=self.portfolio_datasource.all_tickers,
                        window_size=window_size,
                        **risk_return_engine_kwargs,
                    ),
                )
            strat_params["risk_engine"] = self.risk_engines[risk_engine]

            strat_params["portfolio_return_engine"] = self.portfolio_return_engine

            strat_params["frequency"] = self.params["prices_datasource"]["frequency"]
            strat_params["rebalance"] = self.params["prices_datasource"]["rebalance"]
            strat_params["universe"] = self.portfolio_datasource.all_tickers
            strat_params["trans_cost"] = self.params["trans_cost"]
            strat_params["weight_constraint"] = weight_constraint
            self.params["allocation_limit"]["limited_assets"] = np.array(
                [
                    self.portfolio_datasource.all_tickers.index(i)
                    for i in self.params["allocation_limit"]["limited_assets"]
                ]
            )
            self.params["allocation_limit"]["allocate_to"] = (
                "equal"
                if self.params["allocation_limit"]["allocate_to"] == "equal"
                else np.array(
                    [
                        self.portfolio_datasource.all_tickers.index(i)
                        for i in self.params["allocation_limit"]["allocate_to"]
                    ]
                )
            )
            strat_params["scaling"] = self.params["allocation_limit"]
            if strat_params["type"] == "momentum":
                self.strategies[strategy] = momentum.Momentum(strat_params)
            elif strat_params["type"] == "mean_reversion":
                self.strategies[strategy] = mean_reversion.MeanReversion(strat_params)
            elif strat_params["type"] == "pick_all":
                self.strategies[strategy] = pick_all.PickAll(strat_params)
            elif strat_params["type"] == "relative_value":
                self.strategies[strategy] = relative_value.RelativeValue(strat_params)
            elif strat_params["type"] == "magic_formula":
                self.strategies[strategy] = magic_formula.MagicFormula(strat_params)

    def get_weights_constraints_d(self) -> dict:
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
        weight_constraint = self.params["default_weights_constraint"]
        w_consts_d = self.params["weight_constraint"]
        for this_id in self.portfolio_datasource.all_tickers:
            w_consts_d[this_id] = w_consts_d.get(this_id, weight_constraint)
        return w_consts_d

    def run_strategies(self) -> None:
        """
        Run and backtest all strategies
        """
        for date, row in self.prices_datasource.return_data.iterrows():
            r_array = np.array(row)
            current_multiples = self.marketmultiple_datasource.outgoing_row(date)
            current_fundamentals = self.fundamentals_datasource.outgoing_row(date)
            index_components = self.portfolio_datasource.outgoing_row(date)

            assign_dict = {
                "date": date,
                "price_return": r_array,
                "index_comp": index_components,
                "market_caps": current_fundamentals["marketcap"],
                "divyield": current_fundamentals["divyield"],
                "roe": current_fundamentals["roe"],
                "fcfps": current_fundamentals["fcfps"],
                "pe": current_fundamentals["pe"],
                "ps": current_fundamentals["ps"],
                "pb": current_fundamentals["pb"],
                "roic": current_fundamentals["roic"],
                "ebit": current_fundamentals["ebit"],
                "ev": current_fundamentals["ev"],
                "spx_pe": current_multiples["SPX_PE"],
                "spx_pb": current_multiples["SPX_PB"],
                "spx_ps": current_multiples["SPX_PS"],
            }

            for return_engine, return_object in self.return_engines.items():
                return_object.assign(
                    date=date, price_return=r_array, annualize_factor=1
                )

            for risk_engine, risk_object in self.risk_engines.items():
                risk_object.assign(date=date, price_return=r_array, annualize_factor=1)

            self.portfolio_return_engine.assign(
                date=date, price_return=r_array, annualize_factor=1
            )

            # assign returns to strategies and backtest
            for strat, strat_obj in self.strategies.items():
                strat_obj.assign(**assign_dict)

            if date in self.prices_datasource.rebalance_dates:
                logging.log(f"Optimizing {date.strftime('%Y-%m-%d')}")
                for strat, strat_obj in self.strategies.items():
                    if (
                        self.marketmultiple_datasource.is_valid(date)
                        and self.fundamentals_datasource.is_valid(date)
                        and self.portfolio_datasource.is_valid(date)
                        and strat_obj.is_valid()
                    ):
                        strat_obj.backtest(**assign_dict)

                    # reset stop loss engine
                    strat_obj.stop_loss.reset_engine()

                # reset portfolio engines
                self.portfolio_return_engine.reset_engine()

        # assign weights to security objects
        for strat, strat_obj in self.strategies.items():
            for allo, allo_obj in strat_obj.allocation_engines_d.items():
                allocation_df = pd.DataFrame.from_dict(
                    allo_obj.allocations_history,
                    orient="index",
                    columns=self.portfolio_datasource.all_tickers,
                )
                for sec in allocation_df.columns:
                    self.portfolio_datasource.tickers[sec].allocation_df[
                        f"{strat}_{allo}"
                    ] = allocation_df[sec]

    def run(self) -> None:
        """
        run calculations
        """
        logging.log("Start Calculations")
        self.run_strategies()
