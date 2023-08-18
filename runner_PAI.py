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
            self.portfolio_datasource.portfolios[p].calculate_carbon_impact(
                "CARBON_EMISSIONS_SCOPE_1"
            )

    def calculate_pai_1_2(self) -> None:
        """
        Calculate PAI - Mandatory 1.2 - Scope 2 GHG Emissions
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_carbon_impact(
                "CARBON_EMISSIONS_SCOPE_2"
            )

    def calculate_pai_1_3(self) -> None:
        """
        Calculate PAI - Mandatory 1.3 - Scope 3 GHG Emissions
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_carbon_impact(
                "CARBON_EMISSIONS_SCOPE_3_TOTAL"
            )

    def calculate_pai_1_4(self) -> None:
        """
        Calculate PAI - Mandatory 1.4 - Total GHG Emissions
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_carbon_impact(
                "CARBON_EMISSIONS_SCOPE123"
            )

    def calculate_pai_2(self) -> None:
        """
        Calculate PAI - Mandatory 2 - Carbon Footprint (Total GHG Emissions divided by MV per million)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_carboon_footprint()

    def calculate_pai_3(self) -> None:
        """
        Calculate PAI - Mandatory 3 - GHG Intensity of Investee Companies
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_waci()

    def calculate_pai_4(self) -> None:
        """
        Calculate PAI - Mandatory 4 - Exposure to Companies Active in Fossil Fuel Sector
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_fossil_fuel()

    def calculate_pai_5(self) -> None:
        """
        Calculate PAI - Mandatory 5 - Share of Non-Renewable Energy Consumption and Production
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_non_renewable_energy()

    def calculate_pai_6a(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Agriculture, Foresty, Fishing)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("A")

    def calculate_pai_6b(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Mining and Quarrying)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("B")

    def calculate_pai_6c(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Manufacturing)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("C")

    def calculate_pai_6d(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Electricity, Gas Steam and Air Conditioning Supply)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("D")

    def calculate_pai_6e(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Manufacturing)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("E")

    def calculate_pai_6f(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Contruction)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("F")

    def calculate_pai_6g(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Wholesale and Retail Trade)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("G")

    def calculate_pai_6h(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Water Transport)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("H")

    def calculate_pai_6l(self) -> None:
        """
        Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
        (Real Estate Activities)
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_energy_consumption("L")

    def calculate_pai_7(self) -> None:
        """
        Calculate PAI - Mandatory 7 - Activities Negatively Affecting Biodiversity-Sensitive Areas
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_biodiversity()

    def calculate_pai_8(self) -> None:
        """
        Calculate PAI - Mandatory 8 - Emissions to Water
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_water_emissions()

    def run(self) -> None:
        """
        run calculations
        """
        self.calculate_portfolio_value()
        self.calculate_pai_1_1()
        self.calculate_pai_1_2()
        self.calculate_pai_1_3()
        self.calculate_pai_1_4()
        self.calculate_pai_2()
        self.calculate_pai_3()
        self.calculate_pai_4()
        self.calculate_pai_5()
        self.calculate_pai_6a()
        self.calculate_pai_6b()
        self.calculate_pai_6c()
        self.calculate_pai_6d()
        self.calculate_pai_6e()
        self.calculate_pai_6f()
        self.calculate_pai_6g()
        self.calculate_pai_6h()
        self.calculate_pai_6l()
        self.calculate_pai_7()
        self.calculate_pai_8()
