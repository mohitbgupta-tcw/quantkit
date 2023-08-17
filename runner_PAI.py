import pandas as pd
import quantkit.loader.runner as loader
import quantkit.utils.logging as logging


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

        # iterate over dataframes and create objects
        self.iter()

    def iter(self) -> None:
        """
        iterate over DataFrames and create connected objects
        """
        self.iter_securitized_mapping()
        self.iter_regions()
        self.iter_sectors()
        self.iter_portfolios()
        self.iter_securities()
        self.iter_holdings()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()

    def calculate_portfolio_value(self) -> None:
        """
        Calculate PAI - Mandatory 1.1 - Scope 1 GHG Emissions
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_portfolio_value(
                self.params["exchange_rate"]
            )

    def calculate_pai_1_1(self) -> None:
        """
        Calculate PAI - Mandatory 1.1 - Scope 1 GHG Emissions
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_impact(
                "CARBON_EMISSIONS_SCOPE_1"
            )

    def calculate_pai_1_2(self) -> None:
        """
        Calculate PAI - Mandatory 1.1 - Scope 1 GHG Emissions
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_impact(
                "CARBON_EMISSIONS_SCOPE_2"
            )

    def run(self) -> None:
        """
        run calculations
        """
        self.calculate_portfolio_value()
        self.calculate_pai_1_1()
        self.calculate_pai_1_2()
