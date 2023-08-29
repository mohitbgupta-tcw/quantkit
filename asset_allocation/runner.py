import pandas as pd
import quantkit.loader.runner as loader
import quantkit.utils.logging as logging
import quantkit.data_sources.snowflake as snowflake
import quantkit.finance.data_sources.quandl_datasource.quandl_datasource as quds
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.asset_allocation.strategies.momentum as momentum


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

    def init_strategies(self) -> None:
        for strategy in self.params["strategies"]:
            strat_params = self.params["strategies"][strategy]
            strat_params["frequency"] = self.params["quandl_datasource_prices"][
                "frequency"
            ]
            strat_params["universe"] = self.universe
            if strat_params["type"] == "momentum":
                self.strategies[strategy] = momentum.Momentum(strat_params)

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

    def iter_quandl(self) -> None:
        """
        iterate over quandl data
        - attach quandl information to company in self.quandl_information
        - if company doesn't have data, attach all nan's
        """
        # load quandl data
        self.quandl_datasource_prices.load(self.universe_tickers)
        self.quandl_datasource.load(self.universe_tickers)
        self.quandl_datasource.iter(self.universe)
        self.quandl_datasource_prices.iter(self.universe)

    def create_universe(self) -> None:
        """
        Load Portfolio Data from snowflake and create universe
        based on indexes from params file.
        """
        snowflake_params = self.params["API_settings"]["snowflake_parameters"]
        snowflake_params["schema"] = "TIM_SCHEMA"
        table = "Sustainability_Framework_Detailed"
        sf = snowflake.Snowflake(table_name=table, **snowflake_params)
        sf.load()
        self.sust_data = sf.df
        self.sust_data = self.sust_data
        self.universe_tickers = list(
            self.sust_data[
                (self.sust_data["Portfolio ISIN"].isin(self.params["universe"]))
                & (
                    self.sust_data["SCLASS_Level2"].isin(
                        ["Transition", "Sustainable Theme"]
                    )
                )
            ]["Ticker"].unique()
        )
        for c in self.portfolio_datasource.companies:
            ticker = self.portfolio_datasource.companies[c].msci_information[
                "ISSUER_TICKER"
            ]
            if ticker in self.universe_tickers:
                self.universe[ticker] = self.portfolio_datasource.companies[c]

        self.num_assets = len(self.universe)

    def run(self) -> None:
        """
        run calculations
        """
        logging.log("Start Calculations")
