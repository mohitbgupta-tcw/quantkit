import pandas as pd
import quantkit.loader.runner as loader
import quantkit.utils.logging as logging
import quantkit.data_sources.snowflake as snowflake
import quantkit.finance.data_sources.quandl_datasource.quandl_datasource as quds
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.asset_allocation.strategies.momentum as momentum
import quantkit.asset_allocation.strategies.pick_all as pick_all
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.utils.snowflake_utils as snowflake_utils
import numpy as np


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

        # connect quandl datasource
        self.quandl_datasource_prices = quds.QuandlDataSource(
            params=self.params["quandl_datasource_prices"],
            api_settings=self.params["API_settings"],
        )
        self.universe = dict()

        self.annualize_factor = mapping_configs.annualize_factor_d.get(
            self.params["quandl_datasource_prices"]["frequency"], 252
        )
        self.rebalance_window = mapping_configs.rebalance_window_d.get(
            self.params["rebalance"]
        )

        self.strategies = dict()

        # iterate over dataframes and create objects
        logging.log("Start Iterating")
        self.iter()

    def iter(self) -> None:
        """
        iterate over DataFrames and create connected objects
        """
        self.iter_regions()
        self.iter_sectors()
        self.iter_securitized_mapping()
        self.iter_portfolios()
        self.iter_securities()
        self.iter_holdings()
        self.create_universe()
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
        for strategy in self.params["strategies"]:
            strat_params = self.params["strategies"][strategy]
            strat_params["frequency"] = self.params["quandl_datasource_prices"][
                "frequency"
            ]
            strat_params["universe"] = self.universe_tickers
            strat_params["rebalance_dates"] = self.rebalance_dates
            strat_params["trans_cost"] = self.params["trans_cost"]
            strat_params["weight_constraint"] = self.params[
                "default_weights_constraint"
            ]
            if strat_params["type"] == "momentum":
                self.strategies[strategy] = momentum.Momentum(strat_params)
            elif strat_params["type"] == "pick_all":
                self.strategies[strategy] = pick_all.PickAll(strat_params)

    def iter_quandl(self) -> None:
        """
        - iterate over quandl data
            - attach quandl information to company in self.quandl_information
        - create price DataFrame
        - create return DataFrame with sorted universe in columns
        """
        # load quandl data
        self.quandl_datasource_prices.load(self.universe_tickers)
        self.quandl_datasource.load(self.universe_tickers)
        self.quandl_datasource.iter(self.universe)
        self.quandl_datasource_prices.iter(self.universe)

        # price data
        self.price_data = self.quandl_datasource_prices.df.pivot(
            index="date", columns="ticker", values="closeadj"
        )

        # return data
        self.return_data = self.price_data.pct_change(1)

        # filter universe for companies that have quandl and msci data
        self.universe_tickers = list(
            set(self.universe.keys())
            & set(self.return_data.columns)
            & set(self.quandl_datasource.df["ticker"])
        )

        # order return data
        self.return_data = self.return_data[self.universe_tickers]

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
        self.market_caps = np.ones(shape=len(self.universe_tickers))

    def create_universe(self) -> None:
        """
        - Load Portfolio Data from snowflake
        - create universe based on indexes from params file
        """
        df = snowflake_utils.load_from_snowflake(
            database="SANDBOX_ESG",
            schema="TIM_SCHEMA",
            table_name="Sustainability_Framework_Detailed",
            local_configs=self.local_configs,
        )
        if self.params["sustainable_universe"]:
            # all tickers in index which are labeled green or blue
            self.universe_tickers = list(
                df[
                    (df["Portfolio ISIN"].isin(self.params["universe"]))
                    & (df["SCLASS_Level2"].isin(["Transition", "Sustainable Theme"]))
                ]["Ticker"].unique()
            )
        else:
            self.universe_tickers = list(
                df[df["Portfolio ISIN"].isin(self.params["universe"])][
                    "Ticker"
                ].unique()
            )
        # filter for securities that match msci ticker
        for c, comp_store in self.portfolio_datasource.companies.items():
            ticker = comp_store.msci_information["ISSUER_TICKER"]
            if ticker in self.universe_tickers:
                self.universe[ticker] = self.portfolio_datasource.companies[c]

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

                df = df[self.universe_tickers]
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
