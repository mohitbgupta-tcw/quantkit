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
        initial_weight = 0
        for s in self.holdings:
            t = self.holdings[s]["object"].parent_store.type
            isin = self.holdings[s]["object"].parent_store.msci_information[
                "ISSUER_ISIN"
            ]
            if t == "company" and not isin == "NoISIN":
                for h in self.holdings[s]["holding_measures"]:
                    total_mkt_value += h["Base Mkt Val"]
                    initial_weight += h["Portfolio_Weight"]

                self.total_market_value_corp = total_mkt_value / exchange_rate
                self.initial_weight_corp = initial_weight

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
        Calculate Exposure to Fossil Fuels
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

    def calculate_biodiversity(self) -> None:
        """
        Calculate Exposure to Fossil Fuels
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
