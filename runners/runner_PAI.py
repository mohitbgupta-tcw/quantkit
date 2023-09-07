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

    def calculate_pai_9(self) -> None:
        """
        Calculate PAI - Mandatory 9 - Hazardous Waste Ratio
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_hazardous_waste()

    def calculate_pai_10(self) -> None:
        """
        Calculate PAI - Mandatory 10 - Violations of UN Global Compact
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_violations_un()

    def calculate_pai_11(self) -> None:
        """
        Calculate PAI - Mandatory 11 - Lack of Processes to Monitor of UNGC and OECD
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_lack_of_process()

    def calculate_pai_12(self) -> None:
        """
        Calculate PAI - Mandatory 12 - Unadjusted Gender Pay Gap
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_gender_pay_gap()

    def calculate_pai_13(self) -> None:
        """
        Calculate PAI - Mandatory 13 - Board Gender Diversity
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_gender_diversity()

    def calculate_pai_14(self) -> None:
        """
        Calculate PAI - Mandatory 14 - Controversial Weapons Exposure
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_controversial_weapons()

    def calculate_pai_15(self) -> None:
        """
        Calculate PAI - Mandatory 15 - GHG Intensity of Investee Countries
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_ghg_intensity()

    def calculate_pai_16(self) -> None:
        """
        Calculate PAI - Mandatory 16 - Investee Countries Subject to Social Violations
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_social_violations()

    def calculate_pai_17(self) -> None:
        """
        Calculate PAI - Mandatory 17 - Real Estate Assets
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_real_estate(
                "FOSSIL_FUELS_REAL_ESTATE"
            )

    def calculate_pai_18(self) -> None:
        """
        Calculate PAI - Mandatory 18 - Real Estate Assets
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_real_estate(
                "ENERGY_INEFFICIENT_REAL_ESTATE"
            )

    def calculate_additional_environmental(self) -> None:
        """
        Calculate PAI - Additional Environmental - Investments in companies without carbon emission reduction initiatives
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[
                p
            ].calculate_no_carbon_emission_target()

    def calculate_additional_social(self) -> None:
        """
        Calculate PAI - Additional Social - Workplace Accident Prevention Policy
        """
        for p in self.portfolio_datasource.portfolios:
            self.portfolio_datasource.portfolios[p].calculate_workplace_accident()

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
        self.calculate_pai_9()
        self.calculate_pai_10()
        self.calculate_pai_11()
        self.calculate_pai_12()
        self.calculate_pai_13()
        self.calculate_pai_14()
        self.calculate_pai_15()
        self.calculate_pai_16()
        self.calculate_pai_17()
        self.calculate_pai_18()
        self.calculate_additional_environmental()
        self.calculate_additional_social()
