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

    def calculate_impact(self, impact_column: str) -> None:
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
                            "CARBON_EMISSIONS_SCOPE_1": value,
                            "Portfolio_Weight": weight,
                        }
                    )

        impact = 0
        for s in data:
            norm_weight = s["Portfolio_Weight"] / total_weight
            investor_stake = (
                norm_weight * self.total_market_value_corp / (s["EVIC_EUR"] * 1000000)
            )
            impact += s["CARBON_EMISSIONS_SCOPE_1"] * investor_stake
        coverage = total_weight / self.initial_weight_corp

        self.impact_data[impact_column] = {
            "impact": impact,
            "coverage": coverage,
            "data": data,
        }
