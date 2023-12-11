import pandas as pd
import numpy as np
import quantkit.runners.runner as loader
import quantkit.utils.logging as logging
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.asset_allocation.strategies.momentum as momentum
import quantkit.asset_allocation.strategies.pick_all as pick_all
import quantkit.asset_allocation.strategies.relative_value as relative_value
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.asset_allocation.universe.universe as universe_datasource


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
        super().init(local_configs, "asset_allocation")

        self.annualize_factor = mapping_configs.annualize_factor_d.get(
            self.params["prices_datasource"]["frequency"], 252
        )
        self.rebalance_window = mapping_configs.rebalance_window_d.get(
            self.params["prices_datasource"]["rebalance"]
        )

        self.strategies = dict()

        # connect portfolio datasource
        self.portfolio_datasource = universe_datasource.Universe(
            params=self.params["universe_datasource"], api_settings=self.api_settings
        )

        # iterate over dataframes and create objects
        logging.log("Start Iterating")
        self.iter()

    def iter(self) -> None:
        """
        iterate over DataFrames and create connected objects
        """
        self.iter_parent_issuers()
        self.iter_portfolios()
        self.iter_msci()
        self.iter_prices()
        self.iter_fundamentals()
        self.iter_factors()
        self.iter_marketmuliples()
        self.iter_holdings()
        self.iter_securities()
        self.iter_cash()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()
        self.init_strategies()

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
        for strategy, strat_params in self.params["strategies"].items():
            strat_params["frequency"] = self.params["prices_datasource"]["frequency"]
            strat_params["rebalance"] = self.params["prices_datasource"]["rebalance"]
            strat_params["universe"] = self.portfolio_datasource.all_tickers
            strat_params["rebalance_dates"] = self.prices_datasource.rebalance_dates
            strat_params["trans_cost"] = self.params["trans_cost"]
            strat_params["weight_constraint"] = self.params[
                "default_weights_constraint"
            ]
            if strat_params["type"] == "momentum":
                self.strategies[strategy] = momentum.Momentum(strat_params)
            elif strat_params["type"] == "pick_all":
                self.strategies[strategy] = pick_all.PickAll(strat_params)
            elif strat_params["type"] == "relative_value":
                self.strategies[strategy] = relative_value.RelativeValue(strat_params)

    def run_strategies(self) -> None:
        """
        Run and backtest all strategies
        """
        for date, row in self.prices_datasource.return_data.iterrows():
            r_array = np.array(row)
            current_multiples = self.marketmultiple_datasource.outgoing_row(date)
            current_fundamentals = self.fundamentals_datasource.outgoing_row(date)
            index_components = self.portfolio_datasource.outgoing_row(date)
            factors = self.factor_datasource.outgoing_row(date)

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
                "spx_pe": current_multiples["SPX_PE"],
                "spx_pb": current_multiples["SPX_PB"],
                "spx_ps": current_multiples["SPX_PS"],
            }

            # assign returns to strategies and backtest
            for strat, strat_obj in self.strategies.items():
                strat_obj.assign(**assign_dict)
                strat_obj.backtest(date, current_fundamentals["marketcap"])

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
