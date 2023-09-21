import pandas as pd
import datetime
import quantkit.finance.sectors.sectors as sectors


class PortfolioStore(object):
    """
    Portfolio Object. Stores information such as:
        - portfolio id
        - portfolio name
        - holdings (holds security object as well as holding measure (weight, OAS))
        - holdings dataframe

    Parameters
    ----------
    pf: str
        portfolio id, unique identifier for portfolio/ index
    name: str, optional
        portfolio name
    """

    def __init__(self, pf: str, name: str = None) -> None:
        self.id = pf
        self.name = name
        self.holdings = dict()
        self.impact_data = dict()

    def add_holdings(self, holdings_df: pd.DataFrame) -> None:
        """
        Safe historical holdings of portfolio with date, weight, oas, market value as
        dataframe in self.holdings_df.

        Parameters
        ----------
        holdings_df: pd.DataFrame
            all holdings for this portfolio
        """
        self.holdings_df = holdings_df

    def add_sector(self, sector: sectors.Sector) -> None:
        """
        Attach Sector to portfolio

        Parameters
        ----------
        sector: sectors.Sector
            sector object of portfolio
        """
        self.Sector = sector

    def add_as_of_date(self, as_of_date: datetime.date) -> None:
        """
        Attach As Of Date to portfolio

        Parameters
        ----------
        as_of_date: datetime.date
            as of date of portfolio
        """
        self.as_of_date = as_of_date

    def calculate_portfolio_value(self, exchange_rate: float) -> None:
        """
        Calculate portfolio value for corps and sovereigns by adding Base Mkt Val

        Parameters
        ----------
        exchange_rate: float
            EUR/USD exchange rate
        """
        total_mkt_value = 0
        total_mkt_value_sov = 0
        initial_weight = 0
        initial_weight_sov = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            s1 = self.holdings[s]["object"].parent_store.information["Sector_Level_1"]
            if t == "company" and not s1 == "Cash and Other":
                for h in self.holdings[s]["holding_measures"]:
                    total_mkt_value += h["Base Mkt Val"]
                    initial_weight += h["Portfolio_Weight"]
            elif t == "sovereign" and not s1 == "Cash and Other":
                for h in self.holdings[s]["holding_measures"]:
                    total_mkt_value_sov += h["Base Mkt Val"]
                    initial_weight_sov += h["Portfolio_Weight"]

        self.total_market_value_corp = total_mkt_value / exchange_rate
        self.initial_weight_corp = initial_weight
        self.total_market_value_sov = total_mkt_value_sov / exchange_rate
        self.initial_weight_sov = initial_weight_sov

    def calculate_carbon_impact(self, impact_column: str) -> None:
        """
        Calculate Principal Adverse Impact and Coverage for specific column

        Paramters
        ---------
        impact_column: str
            column name of variable to look at
        """
        self.investor_stake_multiplication(impact_column, impact_column)

    def calculate_carboon_footprint(self) -> None:
        """
        Calculate Carbon Footprint
        (Total GHG Emissions divided by MV per million) of portfolio
        """

        total_emissions = self.impact_data["CARBON_EMISSIONS_SCOPE123"]["impact"]
        carbon_footprint = (
            total_emissions / (self.total_market_value_corp / 1000000)
            if self.total_market_value_corp != 0
            else 0
        )
        self.impact_data["Carbon_Footprint"] = {
            "impact": carbon_footprint,
            "coverage": self.impact_data["CARBON_EMISSIONS_SCOPE123"]["coverage"],
            "data": [],
        }

    def calculate_waci(self) -> None:
        """
        Calculate WACI and Coverage
        """
        self.weight_value_multiplication(
            "CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN", "WACI"
        )

    def calculate_fossil_fuel(self) -> None:
        """
        Calculate Exposure to Fossil Fuels
        """
        self.filter_word("ACTIVE_FF_SECTOR_EXPOSURE", "Yes", "Fossil_Fuel_Exposure")

    def calculate_non_renewable_energy(self) -> None:
        """
        Calculate Share of Non-Renewable Energy Consumption and Production
        """
        self.weight_value_multiplication(
            "PCT_NONRENEW_CONSUMP_PROD", "PCT_NONRENEW_CONSUMP_PROD"
        )

    def calculate_energy_consumption(self, nace_section_code: str) -> None:
        """
        Calculate Energy Consumption and Coverage for specific NACE code

        Paramters
        ---------
        nace_section_code: str
            NACE Section Code
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            nace_code = self.holdings[s]["object"].parent_store.msci_information[
                "NACE_SECTION_CODE"
            ]
            energy = self.holdings[s]["object"].parent_store.msci_information[
                "ENERGY_CONSUMP_INTEN_EUR"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if (
                    not (pd.isna(nace_code) or pd.isna(energy) or weight == 0)
                    and t == "company"
                    and nace_code == nace_section_code
                ):
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "NACE_SECTION_CODE": nace_code,
                            "ENERGY_CONSUMP_INTEN_EUR": energy,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            impact += s["ENERGY_CONSUMP_INTEN_EUR"] * norm_weight
        coverage = (
            total_weight / self.initial_weight_corp
            if self.initial_weight_corp != 0
            else 0
        )

        self.impact_data[f"Energy_Consumption_{nace_section_code}"] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_biodiversity(self) -> None:
        """
        Calculate Exposure to biodiversty controversies
        """
        self.filter_word("OPS_PROT_BIODIV_CONTROVS", "Yes", "Biodiversity_Controv")

    def calculate_water_emissions(self) -> None:
        """
        Calculate Water Emissions and Coverage
        """
        self.investor_stake_multiplication("WATER_EM_EFF_METRIC_TONS", "WATER_EM", True)

    def calculate_hazardous_waste(self) -> None:
        """
        Calculate exposure to hazardous waste and Coverage
        """
        self.investor_stake_multiplication(
            "HAZARD_WASTE_METRIC_TON", "HAZARD_WASTE", True
        )

    def calculate_violations_un(self) -> None:
        """
        Calculate Violations of UN Global Compact
        """
        self.filter_word("OVERALL_FLAG", "Red", "UN_violations")

    def calculate_lack_of_process(self) -> None:
        """
        Calculate Lack of Processes to Monitor of UNGC and OECD
        """
        self.filter_word(
            "MECH_UN_GLOBAL_COMPACT", "No Evidence", "MECH_UN_GLOBAL_COMPACT"
        )

    def calculate_gender_pay_gap(self) -> None:
        """
        Calculate Unadjusted Gender Pay Gap
        """
        self.weight_value_multiplication("GENDER_PAY_GAP_RATIO", "GENDER_PAY_GAP_RATIO")

    def calculate_gender_diversity(self) -> None:
        """
        Calculate Board Gender Diversity
        """
        self.weight_value_multiplication("FEMALE_DIRECTORS_PCT", "FEMALE_DIRECTORS_PCT")

    def calculate_controversial_weapons(self) -> None:
        """
        Calculate Controversial Weapons Exposure
        """
        self.filter_word(
            "CONTRO_WEAP_CBLMBW_ANYTIE", "Yes", "CONTRO_WEAP_CBLMBW_ANYTIE"
        )

    def calculate_ghg_intensity(self) -> None:
        """
        GHG Intensity of Investee Countries
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "CTRY_GHG_INTEN_GDP_EUR"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "sovereign":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "CTRY_GHG_INTEN_GDP_EUR": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            impact += s["CTRY_GHG_INTEN_GDP_EUR"] * norm_weight
        coverage = (
            total_weight / self.initial_weight_sov
            if self.initial_weight_sov != 0
            else 0
        )

        self.impact_data["CTRY_GHG_INTEN_GDP_EUR"] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_social_violations(self) -> None:
        """
        Calculate Investee Countries Subject to Social Violations
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value1 = self.holdings[s]["object"].parent_store.msci_information[
                "GOVERNMENT_EU_SANCTIONS"
            ]
            value2 = self.holdings[s]["object"].parent_store.msci_information[
                "GOVERNMENT_UN_SANCTIONS"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if (
                    not (pd.isna(value1) or pd.isna(value2) or weight == 0)
                    and t == "sovereign"
                ):
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "GOVERNMENT_EU_SANCTIONS": value1,
                            "GOVERNMENT_UN_SANCTIONS": value2,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if (
                s["GOVERNMENT_EU_SANCTIONS"] == "Yes"
                or s["GOVERNMENT_UN_SANCTIONS"] == "Yes"
            ):
                impact += norm_weight
        coverage = (
            total_weight / self.initial_weight_sov
            if self.initial_weight_sov != 0
            else 0
        )

        self.impact_data["SANCTIONS"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

    def calculate_real_estate(self, impact_column: str) -> None:
        """
        Calculate Real Estate Assets

        Paramters
        ---------
        impact_column: str
            column name of variable to look at
        """

        self.not_applicable(impact_column)

    def calculate_no_carbon_emission_target(self) -> None:
        """
        Calculate Investments in companies without carbon emission reduction initiatives
        """
        self.filter_word(
            "CARBON_EMISSIONS_REDUCT_INITIATIVES",
            "Not Disclosed",
            "CARBON_EMISSIONS_REDUCT_INITIATIVES",
        )

    def calculate_workplace_accident(self) -> None:
        """
        Calculate Workplace Accident Prevention Policy
        """
        self.filter_word(
            "WORKPLACE_ACC_PREV_POL", "Not Disclosed", "WORKPLACE_ACC_PREV_POL"
        )

    def investor_stake_multiplication(
        self, impact_column: str, impact_label: str, scale: bool = False
    ) -> None:
        """
        Take Impact Column and multiply with investor stake

        Paramters
        ---------
        impact_column: str
            column name of variable to look at
        impact_label: str
            label for self.impact_data
        scale: bool, optional
            scale factor for impact
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            evic = self.holdings[s]["object"].parent_store.msci_information["EVIC_EUR"]
            value = self.holdings[s]["object"].parent_store.msci_information[
                impact_column
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if (
                    not (pd.isna(evic) or pd.isna(value) or weight == 0)
                    and t == "company"
                ):
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "EVIC_EUR": evic,
                            impact_column: value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            investor_stake = (
                norm_weight * self.total_market_value_corp / (s["EVIC_EUR"] * 1000000)
            )
            impact += s[impact_column] * investor_stake
        coverage = (
            total_weight / self.initial_weight_corp
            if self.initial_weight_corp != 0
            else 0
        )

        scale_factor = (
            self.total_market_value_corp / 1000000
            if scale and self.total_market_value_corp != 0
            else 1
        )

        self.impact_data[impact_label] = {
            "impact": impact / scale_factor,
            "coverage": coverage,
            "data": data,
        }

    def weight_value_multiplication(
        self, impact_column: str, impact_label: str
    ) -> None:
        """
        Multiply Impact Value with normalized weight

        Paramters
        ---------
        impact_column: str
            column name of variable to look at
        impact_label: str
            label for self.impact_data
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                impact_column
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            impact_column: value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            waci = norm_weight * s[impact_column]
            impact += waci

        coverage = (
            total_weight / self.initial_weight_corp
            if self.initial_weight_corp != 0
            else 0
        )

        self.impact_data[impact_label] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def filter_word(
        self, impact_column: str, filter_word: str, impact_label: str
    ) -> None:
        """
        Filter impact column by specific word

        Paramters
        ---------
        impact_column: str
            column name of variable to look at
        filter_word: str
            word to filter impact column on
        impact_label: str
            label for self.impact_data
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                impact_column
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            impact_column: value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s[impact_column] == filter_word:
                impact += norm_weight
        coverage = (
            total_weight / self.initial_weight_corp
            if self.initial_weight_corp != 0
            else 0
        )

        self.impact_data[impact_label] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

    def not_applicable(self, impact_column: str) -> None:
        """
        Label for not applicable calculation

        Paramters
        ---------
        impact_column: str
            column name of variable to look at
        """

        self.impact_data[impact_column] = {
            "impact": "Not Applicable",
            "coverage": "Not Applicable",
            "data": [],
        }
