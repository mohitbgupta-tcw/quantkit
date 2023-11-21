import pandas as pd
import numpy as np
import quantkit.loader.runner as loader
import quantkit.utils.logging as logging
import quantkit.data_sources.snowflake as snowflake
import quantkit.finance.data_sources.quandl_datasource.quandl_datasource as quds
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.asset_allocation.strategies.momentum as momentum
import quantkit.asset_allocation.strategies.pick_all as pick_all
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.utils.snowflake_utils as snowflake_utils
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
        super().init(local_configs)

        self.annualize_factor = mapping_configs.annualize_factor_d.get(
            self.params["quandl_datasource_prices"]["frequency"], 252
        )
        self.rebalance_window = mapping_configs.rebalance_window_d.get(
            self.params["rebalance"]
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
        self.create_universe()
        self.iter_msci()
        self.iter_quandl()
        self.iter_holdings()
        self.iter_securities()
        self.iter_cash()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()
        self.init_strategies()

    def create_universe(self) -> None:
        """
        - Load Portfolio Data from snowflake
        - create universe based on indexes from params file
        """
        self.portfolio_datasource.load()
        self.portfolio_datasource.iter()

    def iter_quandl(self) -> None:
        """
        - iterate over quandl data
            - attach quandl information to company in self.quandl_information
        - create price DataFrame
        - create return DataFrame with sorted universe in columns
        """
        # load quandl data
        self.params["quandl_datasource"][
            "duplication"
        ] = self.ticker_parent_issuer_datasource.parent_issuers
        super().iter_quandl()

        # price data
        self.price_data = self.quandl_datasource_prices.df.pivot(
            index="date", columns="ticker", values="closeadj"
        )

        # return data
        self.return_data = self.price_data.pct_change(1)

        # check if all securities have data
        quandl = list(self.price_data.columns)
        all_tickers = self.portfolio_datasource.all_tickers
        not_quandl = list(set(all_tickers) - set(quandl))
        if not_quandl:
            raise KeyError(
                f"The following identifiers were not recognized: {not_quandl}"
            )

        # order return data
        self.return_data = self.return_data[self.portfolio_datasource.all_tickers]

        # rebalance dates -> last trading day of month
        self.rebalance_dates = list(
            self.return_data.groupby(
                pd.Grouper(
                    freq=mapping_configs.pandas_translation[self.params["rebalance"]]
                )
            )
            .tail(1)
            .index
        )

        # fundamental dates -> date + 3 months
        self.fundamental_dates = list(
            self.quandl_datasource.df["release_date"].sort_values().unique()
        )
        self.next_fundamental_date = 0

        # initialize market caps
        self.market_caps = np.ones(shape=len(self.portfolio_datasource.all_tickers))

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
            strat_params["frequency"] = self.params["quandl_datasource_prices"][
                "frequency"
            ]
            strat_params["universe"] = self.portfolio_datasource.all_tickers
            strat_params["rebalance_dates"] = self.rebalance_dates
            strat_params["trans_cost"] = self.params["trans_cost"]
            strat_params["weight_constraint"] = self.params[
                "default_weights_constraint"
            ]
            if strat_params["type"] == "momentum":
                self.strategies[strategy] = momentum.Momentum(strat_params)
            elif strat_params["type"] == "pick_all":
                self.strategies[strategy] = pick_all.PickAll(strat_params)

    def run_strategies(self) -> None:
        """
        Run and backtest all strategies
        """
        for date, row in self.return_data.iterrows():
            r_array = np.array(row)

            # assign new market weights each quarter
            if date >= self.fundamental_dates[self.next_fundamental_date]:
                df = self.quandl_datasource.df.pivot(
                    index="release_date", columns="ticker", values="marketcap"
                )
                df = df.loc[self.fundamental_dates[self.next_fundamental_date]]

                df = df[self.portfolio_datasource.all_tickers]
                self.market_caps = np.array(df)
                self.next_fundamental_date += 1

            # assign returns to strategies and backtest
            for strat, strat_obj in self.strategies.items():
                strat_obj.assign(date, r_array)
                strat_obj.backtest(
                    date,
                    self.market_caps,
                    mapping_configs.annualize_factor_d[self.params["rebalance"]],
                )

    def run(self) -> None:
        """
        run calculations
        """
        logging.log("Start Calculations")
        self.run_strategies()
