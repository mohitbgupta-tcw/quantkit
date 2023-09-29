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
        self.iter_portfolios()
        self.iter_msci()
        self.iter_holdings()
        self.iter_securities()
        self.iter_cash()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()

    def calculate_pais(self) -> None:
        """
        - Calculate Portfolio Value
        - Calculate PAI - Mandatory 1.1 - Scope 1 GHG Emissions
        - Calculate PAI - Mandatory 1.2 - Scope 2 GHG Emissions
        - Calculate PAI - Mandatory 1.3 - Scope 3 GHG Emissions
        - Calculate PAI - Mandatory 1.4 - Total GHG Emissions
        - Calculate PAI - Mandatory 2 - Carbon Footprint (Total GHG Emissions divided by MV per million)
        - Calculate PAI - Mandatory 3 - GHG Intensity of Investee Companies
        - Calculate PAI - Mandatory 4 - Exposure to Companies Active in Fossil Fuel Sector
        - Calculate PAI - Mandatory 5 - Share of Non-Renewable Energy Consumption and Production
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Agriculture, Foresty, Fishing)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Mining and Quarrying)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Manufacturing)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Electricity, Gas Steam and Air Conditioning Supply)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Manufacturing)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Contruction)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Wholesale and Retail Trade)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Water Transport)
        - Calculate PAI - Mandatory 6 - Energy Consumption Intensity Per High Impact Climate Sector
                                        (Real Estate Activities)
        - Calculate PAI - Mandatory 7 - Activities Negatively Affecting Biodiversity-Sensitive Areas
        - Calculate PAI - Mandatory 8 - Emissions to Water
        - Calculate PAI - Mandatory 9 - Hazardous Waste Ratio
        - Calculate PAI - Mandatory 10 - Violations of UN Global Compact
        - Calculate PAI - Mandatory 11 - Lack of Processes to Monitor of UNGC and OECD
        - Calculate PAI - Mandatory 12 - Unadjusted Gender Pay Gap
        - Calculate PAI - Mandatory 13 - Board Gender Diversity
        - Calculate PAI - Mandatory 14 - Controversial Weapons Exposure
        - Calculate PAI - Mandatory 15 - GHG Intensity of Investee Countries
        - Calculate PAI - Mandatory 16 - Investee Countries Subject to Social Violations
        - Calculate PAI - Mandatory 17 - Real Estate Assets
        - Calculate PAI - Mandatory 18 - Real Estate Assets
        - Calculate PAI - Additional Environmental - Investments in companies without carbon emission reduction initiatives
        - Calculate PAI - Additional Social - Workplace Accident Prevention Policy
        """
        for p, port_store in self.portfolio_datasource.portfolios.items():
            port_store.calculate_portfolio_value(self.params["exchange_rate"])
            port_store.calculate_carbon_impact("CARBON_EMISSIONS_SCOPE_1")
            port_store.calculate_carbon_impact("CARBON_EMISSIONS_SCOPE_2")
            port_store.calculate_carbon_impact("CARBON_EMISSIONS_SCOPE_3_TOTAL")
            port_store.calculate_carbon_impact("CARBON_EMISSIONS_SCOPE123")
            port_store.calculate_carboon_footprint()
            port_store.calculate_waci()
            port_store.calculate_fossil_fuel()
            port_store.calculate_non_renewable_energy()
            port_store.calculate_energy_consumption("A")
            port_store.calculate_energy_consumption("B")
            port_store.calculate_energy_consumption("C")
            port_store.calculate_energy_consumption("D")
            port_store.calculate_energy_consumption("E")
            port_store.calculate_energy_consumption("F")
            port_store.calculate_energy_consumption("G")
            port_store.calculate_energy_consumption("H")
            port_store.calculate_energy_consumption("L")
            port_store.calculate_biodiversity()
            port_store.calculate_water_emissions()
            port_store.calculate_hazardous_waste()
            port_store.calculate_violations_un()
            port_store.calculate_lack_of_process()
            port_store.calculate_gender_pay_gap()
            port_store.calculate_gender_diversity()
            port_store.calculate_controversial_weapons()
            port_store.calculate_ghg_intensity()
            port_store.calculate_social_violations()
            port_store.calculate_real_estate("FOSSIL_FUELS_REAL_ESTATE")
            port_store.calculate_real_estate("ENERGY_INEFFICIENT_REAL_ESTATE")
            port_store.calculate_no_carbon_emission_target()
            port_store.calculate_workplace_accident()

    def run(self) -> None:
        """
        run calculations
        """
        logging.log("Start Calculations")
        self.calculate_pais()
