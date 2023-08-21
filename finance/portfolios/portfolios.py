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

    def __init__(self, pf: str, name: str = None):
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

        """
        self.holdings_df = holdings_df

    def add_sector(self, sector: sectors.Sector) -> None:
        """
        Attach Sector to portfolio

        Parameters
        ----------
        sector: sectors.Sector
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
        Calculate portfolio value by adding Base Mkt Val

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
            isin = self.holdings[s]["object"].parent_store.msci_information[
                "ISSUER_ISIN"
            ]
            if t == "company" and not isin == "NoISIN":
                for h in self.holdings[s]["holding_measures"]:
                    total_mkt_value += h["Base Mkt Val"]
                    initial_weight += h["Portfolio_Weight"]
            elif t == "sovereign" and not isin == "NoISIN":
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
        coverage = total_weight / self.initial_weight_corp

        self.impact_data[impact_column] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_carboon_footprint(self) -> None:
        """
        Calculate Carbon Footprint
        (Total GHG Emissions divided by MV per million) of portfolio
        """

        total_emissions = self.impact_data["CARBON_EMISSIONS_SCOPE123"]["impact"]
        carbon_footprint = total_emissions / (self.total_market_value_corp / 1000000)
        self.impact_data["Carbon_Footprint"] = {
            "impact": carbon_footprint,
            "coverage": self.impact_data["CARBON_EMISSIONS_SCOPE123"]["coverage"],
        }

    def calculate_waci(self) -> None:
        """
        Calculate WACI and Coverage
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            waci = norm_weight * s["CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN"]
            impact += waci
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["WACI"] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_fossil_fuel(self) -> None:
        """
        Calculate Exposure to Fossil Fuels
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "ACTIVE_FF_SECTOR_EXPOSURE"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "ACTIVE_FF_SECTOR_EXPOSURE": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s["ACTIVE_FF_SECTOR_EXPOSURE"] == "Yes":
                impact += norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["Fossil_Fuel_Exposure"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

    def calculate_non_renewable_energy(self) -> None:
        """
        Calculate Share of Non-Renewable Energy Consumption and Production
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "PCT_NONRENEW_CONSUMP_PROD"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "PCT_NONRENEW_CONSUMP_PROD": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            nonrenew = norm_weight * s["PCT_NONRENEW_CONSUMP_PROD"]
            impact += nonrenew
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["PCT_NONRENEW_CONSUMP_PROD_WEIGHTED"] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_energy_consumption(self, nace_section_code: str) -> None:
        """
        Calculate Principal Adverse Impact and Coverage for specific column

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
        coverage = total_weight / self.initial_weight_corp

        self.impact_data[f"Energy_Consumption_{nace_section_code}"] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_biodiversity(self) -> None:
        """
        Calculate Exposure to biodiversty controversies
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "OPS_PROT_BIODIV_CONTROVS"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "OPS_PROT_BIODIV_CONTROVS": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s["OPS_PROT_BIODIV_CONTROVS"] == "Yes":
                impact += norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["Biodiversity_Controv"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

    def calculate_water_emissions(self) -> None:
        """
        Calculate Principal Adverse Impact and Coverage for specific column
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            evic = self.holdings[s]["object"].parent_store.msci_information["EVIC_EUR"]
            value = self.holdings[s]["object"].parent_store.msci_information[
                "WATER_EM_EFF_METRIC_TONS"
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
                            "WATER_EM_EFF_METRIC_TONS": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            investor_stake = (
                norm_weight * self.total_market_value_corp / (s["EVIC_EUR"] * 1000000)
            )
            impact += s["WATER_EM_EFF_METRIC_TONS"] * investor_stake
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["WATER_EM"] = {
            "impact": impact / (self.total_market_value_corp / 1000000),
            "coverage": coverage,
            "data": data,
        }

    def calculate_hazardous_waste(self) -> None:
        """
        Calculate Principal Adverse Impact and Coverage for specific column
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            evic = self.holdings[s]["object"].parent_store.msci_information["EVIC_EUR"]
            value = self.holdings[s]["object"].parent_store.msci_information[
                "HAZARD_WASTE_METRIC_TON"
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
                            "HAZARD_WASTE_METRIC_TON": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            investor_stake = (
                norm_weight * self.total_market_value_corp / (s["EVIC_EUR"] * 1000000)
            )
            impact += s["HAZARD_WASTE_METRIC_TON"] * investor_stake
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["HAZARD_WASTE"] = {
            "impact": impact / (self.total_market_value_corp / 1000000),
            "coverage": coverage,
            "data": data,
        }

    def calculate_violations_un(self) -> None:
        """
        Calculate Violations of UN Global Compact
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "OVERALL_FLAG"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "OVERALL_FLAG": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s["OVERALL_FLAG"] == "Red":
                impact += norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["UN_violations"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

    def calculate_lack_of_process(self) -> None:
        """
        Calculate Lack of Processes to Monitor of UNGC and OECD
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "MECH_UN_GLOBAL_COMPACT"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "MECH_UN_GLOBAL_COMPACT": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s["MECH_UN_GLOBAL_COMPACT"] == "No evidence":
                impact += norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["MECH_UN_GLOBAL_COMPACT"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

    def calculate_gender_pay_gap(self) -> None:
        """
        Calculate Unadjusted Gender Pay Gap
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "GENDER_PAY_GAP_RATIO"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "GENDER_PAY_GAP_RATIO": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            impact += s["GENDER_PAY_GAP_RATIO"] * norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["GENDER_PAY_GAP_RATIO"] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_gender_diversity(self) -> None:
        """
        Calculate Board Gender Diversity
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "FEMALE_DIRECTORS_PCT"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "FEMALE_DIRECTORS_PCT": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            impact += s["FEMALE_DIRECTORS_PCT"] * norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["FEMALE_DIRECTORS_PCT"] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }

    def calculate_controversial_weapons(self) -> None:
        """
        Calculate Controversial Weapons Exposure
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "CONTRO_WEAP_CBLMBW_ANYTIE"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "CONTRO_WEAP_CBLMBW_ANYTIE": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s["CONTRO_WEAP_CBLMBW_ANYTIE"] == "Yes":
                impact += norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["CONTRO_WEAP_CBLMBW_ANYTIE"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

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

        self.impact_data[impact_column] = {
            "impact": "Not Applicable",
            "coverage": "Not Applicable",
            "data": [],
        }

    def calculate_no_carbon_emission_target(self) -> None:
        """
        Calculate Investments in companies without carbon emission reduction initiatives
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "CARBON_EMISSIONS_REDUCT_INITIATIVES"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "CARBON_EMISSIONS_REDUCT_INITIATIVES": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s["CARBON_EMISSIONS_REDUCT_INITIATIVES"] == "Not Disclosed":
                impact += norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["CARBON_EMISSIONS_REDUCT_INITIATIVES"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }

    def calculate_workplace_accident(self) -> None:
        """
        Calculate Workplace Accident Prevention Policy
        """
        data = list()
        total_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            value = self.holdings[s]["object"].parent_store.msci_information[
                "WORKPLACE_ACC_PREV_POL"
            ]
            for h in self.holdings[s]["holding_measures"]:
                weight = h["Portfolio_Weight"]

                if not (pd.isna(value) or weight == 0) and t == "company":
                    total_weight += weight
                    data.append(
                        {
                            "ISIN": s,
                            "WORKPLACE_ACC_PREV_POL": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            if s["WORKPLACE_ACC_PREV_POL"] == "Not Disclosed":
                impact += norm_weight
        coverage = total_weight / self.initial_weight_corp

        self.impact_data["WORKPLACE_ACC_PREV_POL"] = {
            "impact": impact * 100,
            "coverage": coverage,
            "data": data,
        }
