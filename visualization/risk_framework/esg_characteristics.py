from dash import html
import pandas as pd
import numpy as np
import quantkit.visualization.pdf_creator.visualizor.visualizor as visualizor
import quantkit.utils.portfolio_utils as portfolio_utils


class ESGCharacteristics(visualizor.PDFCreator):
    """
    Main Class to build ESG Characteristics dashboard

    Parameters
    ----------
    title: str
        title of app
    data: pd.DataFrame
        DataFrame with data to be displayed in pdf
    """

    def __init__(self, title: str, data: pd.DataFrame, portfolio: str, benchmark: str):
        super().__init__(title, data)
        self.portfolio_isin = portfolio
        self.benchmark_isin = benchmark

    def run(self) -> None:
        """
        Run and create the app showing benchmark and portfolio data
        benchmark and portfolio should be included in data column "Portfolio ISIN"
        """
        self.portfolio_data = self.data[
            self.data["Portfolio ISIN"] == self.portfolio_isin
        ]
        self.portfolio_name = self.portfolio_data["Portfolio Name"].values[0]
        self.benchmark_data = self.data[
            self.data["Portfolio ISIN"] == self.benchmark_isin
        ]
        self.benchmark_name = self.benchmark_data["Portfolio Name"].values[0]
        self.as_of_date = self.portfolio_data["As Of Date"].max()
        super().run()

    def add_header(self) -> html.Div:
        """
        Create Header including:
            - portfolio name
            - benchmark name
            - as of date of portfolio

        Returns
        -------
        html.Div
            row with header Div
        """
        header_text = [
            html.H5("Sustainable Characteristics", id="overall-header"),
            html.H6(f"Portfolio: {self.portfolio_name}"),
            html.H6(f"Benchmark: {self.benchmark_name}"),
            html.H6(f"As of: {self.as_of_date}"),
        ]
        return super().add_header(header_text)

    def add_footnote(self) -> html.Div:
        """
        Create Footer including;
            - sources
            - disclosures

        Returns
        -------
        html.Div
            row with footer Div
        """
        footnote_text = [
            "Source: TCW, Bloomberg, MSCI, ISS",
            html.Br(),
            html.B("1 Weighted Average Carbon Intensity"),
            """ represents the MV weighted portfolio company’s most recently reported or estimated Scope 1 and 2 emissions normalized by the most recently available sales in $M. Applies to corporates and quasi-sovereigns. """,
            html.B("2 Every issuer or company is assigned a risk score"),
            """ from 1 (leader) to 5 (laggard) across Environmental & Social, Governance, and Transition risks. """,
            html.B("3 Risk Overall"),
            """ is classified as “Leading” if the average score the 3 dimensions  is less than or equal to 2, “Average” if the average score is less than or equal to 4 and greater than 2, and “Poor” if any of scores is a laggard (5). Lack of data disclosures may result in a “Poor” score. """,
            html.B("Not scored"),
            """ securities represent securities that are not evaluated for the purposes of sustainable characteristics. These include cash, cash equivalents, or other instruments that are used for the purposes of portfolio liquidity and hedging only. Portfolio market value is calculated on a trade date basis. A negative market value represents forward settling trades (specifically agency MBS TBAs) that are backed by liquid securities other than cash. """,
            html.B("4 Data is based on TCW analyst assessment"),
            """ which is informed by third party classification of product and services, fundamental analysis of companies, and engagement. """,
        ]
        return super().add_footnote(footnote_text)

    def add_body(self) -> html.Div:
        """
        Create main body with tables, charts and text

        Returns
        -------
        html.Div
            row with body Div
        """
        body_content = [
            # first column
            html.Div(
                [
                    self.add_WACI_table(),
                    self.add_risk_score_distribution(),
                    self.add_score_distribution(),
                ],
                className="three columns",
            ),
            # second column
            html.Div(
                [
                    self.add_planet_table(),
                    self.add_people_table(),
                    self.add_total_table(),
                    self.add_transition_table(),
                ],
                className="five columns",
            ),
            # third column
            html.Div(
                [self.add_carbon_intensity_chart()],
                className="four columns",
            ),
        ]
        return super().add_body(body_content)

    def add_WACI_table(self) -> html.Div:
        """
        For portfolio and benchmark, calculate the WACI and carbon reduction.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with WACI table and header
        """
        waci_portfolio = portfolio_utils.calculate_portfolio_waci(self.portfolio_data)
        waci_index = portfolio_utils.calculate_portfolio_waci(self.benchmark_data)
        carbon_reduction = waci_portfolio / waci_index - 1

        df_WACI = pd.DataFrame(
            data={
                "Name": ["Portfolio", "Index", "Carbon Reduction"],
                "Value": [
                    "{0:.2f}".format(waci_portfolio),
                    "{0:.2f}".format(waci_index),
                    format(carbon_reduction, ".0%"),
                ],
            }
        )

        waci_div = html.Div(
            [
                html.H6(
                    [
                        "Weighted Average Carbon Intensity - ",
                        html.Br(),
                        "Tons CO",
                        html.Sub(2),
                        "e/$M Sales",
                        html.Sup(1, className="superscript"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(df_WACI),
            ],
            className="row",
            id="waci-table",
        )
        return waci_div

    def add_risk_score_distribution(self) -> html.Div:
        """
        For portfolio and benchmark, calculate Average scores.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with scores table and header
        """
        esrm_portfolio = portfolio_utils.calculate_portfolio_esrm(self.portfolio_data)
        esrm_index = portfolio_utils.calculate_portfolio_esrm(self.benchmark_data)
        gov_portfolio = portfolio_utils.calculate_portfolio_governance(
            self.portfolio_data
        )
        gov_index = portfolio_utils.calculate_portfolio_governance(self.benchmark_data)
        trans_portfolio = portfolio_utils.calculate_portfolio_transition(
            self.portfolio_data
        )
        trans_index = portfolio_utils.calculate_portfolio_transition(
            self.benchmark_data
        )

        total_portfolio = (esrm_portfolio + gov_portfolio + trans_portfolio) / 3
        total_index = (esrm_index + gov_index + trans_index) / 3

        df_risk_score_distr = pd.DataFrame(
            data={
                "": ["E&S", "Governance", "Transition", "Overall"],
                "Portfolio": [
                    "{0:.2f}".format(esrm_portfolio),
                    "{0:.2f}".format(gov_portfolio),
                    "{0:.2f}".format(trans_portfolio),
                    "{0:.2f}".format(total_portfolio),
                ],
                "Index": [
                    "{0:.2f}".format(esrm_index),
                    "{0:.2f}".format(gov_index),
                    "{0:.2f}".format(trans_index),
                    "{0:.2f}".format(total_index),
                ],
            }
        )

        distr_div = html.Div(
            [
                html.H6(
                    [
                        "TCW ESG Risk Score Distribution",
                        html.Sup(2, className="superscript"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(
                    df_risk_score_distr, show_vertical_lines=True, show_header=True
                ),
            ],
            className="row",
            id="esg-scores",
        )
        return distr_div

    def add_score_distribution(self) -> html.Div:
        """
        For a portfolio, calculate score distribution.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with scores distribution table and header
        """
        scores = portfolio_utils.calculate_risk_distribution(self.portfolio_data)
        total = sum(scores.values())

        df_distr = pd.DataFrame(
            data={
                "Name": [
                    "Leading ESG Score",
                    "Average ESG Score",
                    "Poor Risk Score",
                    "Not Scored",
                    "Total",
                ],
                "Value": [
                    "{0:.2f}".format(scores["Leading ESG Score"]),
                    "{0:.2f}".format(scores["Average ESG Score"]),
                    "{0:.2f}".format(scores["Poor Risk Score"]),
                    "{0:.2f}".format(scores["Not Scored"]),
                    "{0:.2f}".format(total),
                ],
            }
        )
        distr = html.Div(
            [
                html.H6(
                    [
                        "TCW Score Distribution (% MV)",
                        html.Sup(3, className="superscript"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(df_distr),
            ],
            className="row",
            id="score-distribution",
        )
        return distr

    def add_planet_table(self) -> html.Div:
        """
        For a portfolio, calculate score distribution for planet themes.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with scores distribution table
        """
        scores = portfolio_utils.calculate_planet_distribution(self.portfolio_data)
        sust_total = sum(scores.values())

        planet_table = pd.DataFrame(
            data={
                "Name": [
                    "Renewable Energy, Storage, and Green Hydrogen",
                    "Sustainable Mobility",
                    "Circular Economy",
                    "Climate Change Adaption & Risk Management",
                    "Biodiversity & Sustainable Land & Water Use",
                    "Sustainable Real Assets & Smart Cities",
                    "Total Planet Themes",
                ],
                "Value": [
                    "{0:.2f}".format(scores["RENEWENERGY"]),
                    "{0:.2f}".format(scores["MOBILITY"]),
                    "{0:.2f}".format(scores["CIRCULARITY"]),
                    "{0:.2f}".format(scores["CCADAPT"]),
                    "{0:.2f}".format(scores["BIODIVERSITY"]),
                    "{0:.2f}".format(scores["SMARTCITIES"]),
                    "{0:.2f}".format(sust_total),
                ],
            }
        )

        t = self.add_table(
            planet_table, add_vertical_column="Planet", id="planet-table"
        )

        sust_table = html.Div(
            [
                html.H6(
                    ["Sustainable Themes (% MV)", html.Sup(4, className="superscript")],
                    className="subtitle padded",
                ),
                t,
            ],
            className="row",
        )
        return sust_table

    def add_people_table(self) -> html.Div:
        """
        For a portfolio, calculate score distribution for people themes.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with scores distribution table
        """
        scores = portfolio_utils.calculate_people_distribution(self.portfolio_data)
        sust_total = sum(scores.values())

        people_table = pd.DataFrame(
            data={
                "Name": [
                    "Health",
                    "Sanitation and Hygiene",
                    "Education",
                    "Financial and Digital Inclusion",
                    "Nutrition",
                    "Affordable and Inclusive Housing",
                    "Total People Themes",
                ],
                "Value": [
                    "{0:.2f}".format(scores["HEALTH"]),
                    "{0:.2f}".format(scores["SANITATION"]),
                    "{0:.2f}".format(scores["EDU"]),
                    "{0:.2f}".format(scores["INCLUSION"]),
                    "{0:.2f}".format(scores["NUTRITION"]),
                    "{0:.2f}".format(scores["AFFORDABLE"]),
                    "{0:.2f}".format(sust_total),
                ],
            }
        )

        t = self.add_table(
            people_table, add_vertical_column="People", id="people-table"
        )

        sust_table = html.Div(
            [t],
            className="row",
        )
        return sust_table

    def add_total_table(self) -> html.Div:
        """
        For a portfolio, calculate the total portfolio weight for sustainable themes
        Show these values in a table.

        Returns
        -------
        html.Div
            div with total score table
        """
        scores_people = portfolio_utils.calculate_people_distribution(
            self.portfolio_data
        )
        scores_planet = portfolio_utils.calculate_planet_distribution(
            self.portfolio_data
        )
        total = sum(scores_people.values()) + sum(scores_planet.values())
        total_table = pd.DataFrame(
            data={
                "Name": ["Total Sustainable Themes"],
                "Value": [
                    "{0:.2f}".format(total),
                ],
            }
        )

        t = self.add_table(total_table, add_vertical_column=" ", id="total-table")

        sust_table = html.Div(
            [t],
            className="row",
        )
        return sust_table

    def add_transition_table(self) -> html.Div:
        """
        For a portfolio, calculate score distribution for transition themes.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with scores distribution table
        """
        scores = portfolio_utils.calculate_transition_distribution(self.portfolio_data)
        sust_total = sum(scores.values())

        transition_table = pd.DataFrame(
            data={
                "Name": [
                    "Low Carbon Energy, Power, and Non-Green Hydrogen",
                    "Pivoting Transport",
                    "Materials in Transition",
                    "Carbon Accounting, Removal, & Green Finance",
                    "Improving Agriculture & Forestry",
                    "Transitioning Real Assets & Infrastructure",
                    "Total Transition Themes",
                ],
                "Value": [
                    "{0:.2f}".format(scores["LOWCARBON"]),
                    "{0:.2f}".format(scores["PIVOTTRANSPORT"]),
                    "{0:.2f}".format(scores["MATERIALS"]),
                    "{0:.2f}".format(scores["CARBONACCOUNT"]),
                    "{0:.2f}".format(scores["AGRIFORESTRY"]),
                    "{0:.2f}".format(scores["REALASSETS"]),
                    "{0:.2f}".format(sust_total),
                ],
            }
        )
        t = self.add_table(
            transition_table, add_vertical_column="Transition", id="transition-table"
        )

        sust_table = html.Div(
            [
                html.H6(
                    [
                        "Transition Themes (% MV)",
                    ],
                    className="subtitle padded",
                ),
                t,
            ],
            className="row",
        )
        return sust_table

    def add_carbon_intensity_chart(self) -> html.Div:
        """
        For a portfolio, calculate carbon intensity per sector.
        Show these values in a bar chart.

        Returns
        -------
        html.Div
            div with bar chart and header
        """
        df_ci = portfolio_utils.calculate_carbon_intensity(self.portfolio_data)
        df_ci["Sector"] = df_ci["Sector"].str.replace(" ", "   <br>")
        df_ci["Carbon_Intensity"] = round(df_ci["Carbon_Intensity"], 0)

        ci_chart = html.Div(
            [
                html.H6(
                    "Carbon Intensity by Sector",
                    className="subtitle padded",
                ),
                self.add_bar_chart(x=df_ci["Carbon_Intensity"], y=df_ci["Sector"]),
            ],
            className="row carbon-intensity",
        )
        return ci_chart
