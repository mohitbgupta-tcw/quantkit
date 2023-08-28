from dash import html
import pandas as pd
import numpy as np
import datetime
import quantkit.visualization.pdf_creator.visualizor.visualizor as visualizor
import quantkit.utils.portfolio_utils as portfolio_utils
import quantkit.utils.mapping_configs as mapping_utils
from typing import Union
from copy import deepcopy
import math


class ESGCharacteristics(visualizor.PDFCreator):
    """
    Main Class to build ESG Characteristics dashboard

    Parameters
    ----------
    title: str
        title of app
    data: pd.DataFrame
        DataFrame with data to be displayed in pdf
    portfolio_type: str
        type of portfolio, either "equity", "fixed_income", or "em"
    portfolio: str | int
        portfolio ISIN
    benchmark: str | int
        benchmark ISIN
    filtered: bool, optional
        filter portfolio holdings for corporates and quasi-sovereigns
    """

    def __init__(
        self,
        title: str,
        data: pd.DataFrame,
        portfolio_type: str,
        portfolio: Union[str, int],
        benchmark: Union[str, int],
        filtered: bool = False,
    ):
        super().__init__(title, data)
        self.portfolio_type = portfolio_type
        self.portfolio_isin = portfolio
        self.benchmark_isin = benchmark
        self.waci_benchmark_isin = (
            "JPM EM Custom Index (50/50)" if portfolio == 3750 else self.benchmark_isin
        )
        self.filtered = filtered
        self.portfolio_divider = 28 if self.filtered else 24

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
        self.waci_benchmark_data = self.data[
            self.data["Portfolio ISIN"] == self.waci_benchmark_isin
        ]
        self.benchmark_name = (
            self.benchmark_data["Portfolio Name"].values[0]
            if len(self.benchmark_data) > 0
            else self.benchmark_isin
        )
        self.as_of_date = datetime.datetime.strptime(
            self.portfolio_data["As Of Date"].max(), "%m/%d/%Y"
        ).date()
        super().run()

    def create_layout(self) -> html.Div:
        """
        Create the layout and pages of the pdf
        """
        all_pages = list()
        all_pages.append(
            self.create_page(
                self.add_header(), self.add_body(), self.add_footnote(), page_no=0
            )
        )

        # if self.filtered:
        #     self.portfolio_data = self.portfolio_data[
        #         self.data["Sector Level 2"].isin(
        #             [
        #                 "Financial Institution",
        #                 "Industrial",
        #                 "Quasi Sovereign",
        #                 "Utility",
        #             ]
        #         )
        #     ]

        # for i in range(math.ceil(len(self.portfolio_data) / self.portfolio_divider)):
        #     all_pages.append(
        #         self.create_page(
        #             self.add_header_holdings(),
        #             self.add_body_portfolio_holdings(page_no=i),
        #             html.Div(),
        #             page_no=i + 1,
        #         )
        #     )

        pages_div = html.Div(all_pages)
        return pages_div

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
            html.H3("Sustainable Characteristics", className="overall-header"),
            html.H5(f"{self.portfolio_name}"),
            html.P(
                f"A SUB-FUND OF TCW FUNDS, A LUXEMBOURG DOMICILED UCITS",
                className="sub-header",
            ),
            html.P(
                f"""AS OF {self.as_of_date.strftime("%d %B %Y")} | SHARE CLASS: IU""",
                className="sub-header",
            ),
        ]
        return super().add_header(header_text)

    def add_header_holdings(self) -> html.Div:
        """
        Create Header for Holdings page including:
            - portfolio name
            - benchmark name
            - as of date of portfolio

        Returns
        -------
        html.Div
            row with header Div
        """
        if not self.filtered:
            header = "Portfolio Holdings"
        else:
            header = "Portfolio Holdings (Corporates and Quasi-Sovereigns only)"
        header_text = [
            html.H3(header, className="overall-header"),
            html.H5(f"{self.portfolio_name}"),
            html.P(
                f"A SUB-FUND OF TCW FUNDS, A LUXEMBOURG DOMICILED UCITS",
                className="sub-header",
            ),
            html.P(
                f"""AS OF {self.as_of_date.strftime("%d %B %Y")} | SHARE CLASS: IU""",
                className="sub-header",
            ),
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
        if self.portfolio_type == "em":
            index_text = mapping_utils.benchmark_text[self.benchmark_isin].split(":", 1)
            footnote_text = [
                "Source: TCW, Bloomberg, MSCI, ISS",
                html.Br(),
                html.B("1 Sustainable Investments:"),
                """ TCW utilizes proprietary tools and research to identify holdings that can be considered sustainable, either because these assets align with our classification of a sustainably managed entity or because the products, services, or representative collateral aligns with the sustainable themes outlined above. """,
                html.B("2 Other securities"),
                """ represent securities that are not evaluated for the purposes of sustainable characteristics. These securities are allowed up to 20% of the portfolio and include cash, cash equivalents, or other instruments that are used for the purposes of portfolio liquidity and hedging only. Portfolio market value is calculated on a trade date basis. A negative market value represents forward settling trades (specifically agency MBS TBAs) that are backed by liquid securities other than cash. """,
                html.B("3 Weighted Average Carbon Intensity measure"),
                """ represents the weighted average summary of the portfolio company’s most recently reported or estimated Scope 1 and 2 emissions normalized by the most recently available sales in million USD. """,
                html.B(
                    "4 With respect to the specific sustainable investment objective"
                ),
                """ of reducing the carbon intensity of the corporate and quasi-sovereign holdings relative to the broader representative universe of Emerging Market corporate and quasi sovereign holdings, this Sub-Fund utilizes a custom combination of the JP Morgan CEMBI Broad Diversified Index and quasi-sovereign issuers in the JP Morgan EMBI Global Diversified Index to determine the appropriate constituents.""",
                html.B("5 Carbon intensity reduction"),
                """ relative to custom benchmark. """,
                html.Br(className="m"),
                html.Span(
                    [
                        html.B(index_text[0] + ":"),
                        index_text[1],
                    ],
                ),
            ]
        elif self.portfolio_type == "fixed_income_a8":
            index_text = mapping_utils.benchmark_text[self.benchmark_isin].split(":", 1)
            footnote_text = [
                "Source: TCW, Bloomberg, MSCI, ISS",
                html.Br(),
                html.B("1 Sustainable Investments:"),
                """ TCW utilizes proprietary tools and research to identify holdings that can be considered sustainable, either because these assets align with our classification of a sustainably managed entity or because the products, services, or representative collateral aligns with the sustainable themes outlined above. """,
                html.B("2 Other securities"),
                """ represent securities that are not evaluated for the purposes of sustainable characteristics. These securities are allowed up to 20% of the portfolio and include cash, cash equivalents, or other instruments that are used for the purposes of portfolio liquidity and hedging only. Portfolio market value is calculated on a trade date basis. A negative market value represents forward settling trades (specifically agency MBS TBAs) that are backed by liquid securities other than cash. """,
                html.B("3 Weighted Average Carbon Intensity measure"),
                """ represents the weighted average summary of the portfolio company’s most recently reported or estimated Scope 1 and 2 emissions normalized by the most recently available sales in million USD. """,
                html.B("4 Carbon intensity reduction"),
                """ relative to benchmark and/or universe. Applies to equity securities. """,
                html.B("5 TCW has developed a scoring methodology"),
                """ to assess the ESG risk or characteristics of companies and/or issuers. All securities are assessed according to this methodology. """,
                html.B("6 Not scored securities"),
                """ represent securities that are not evaluated for the purposes of sustainable characteristics. These include cash, cash equivalents, or other instruments that are used for the purposes of portfolio liquidity and hedging only. Portfolio market value is calculated on a trade date basis. A negative market value represents forward settling trades (specifically agency MBS TBAs) that are backed by liquid securities other than cash.""",
                html.Br(className="m"),
                html.Span(
                    [
                        html.B(index_text[0] + ":"),
                        index_text[1],
                    ],
                ),
            ]
        elif self.portfolio_type in ["equity_a9", "fixed_income_a9"]:
            index_text = mapping_utils.benchmark_text[self.benchmark_isin].split(":", 1)
            footnote_text = [
                "Source: TCW, Bloomberg, MSCI, ISS",
                html.Br(),
                html.B("1 Sustainable Investments:"),
                """ TCW utilizes proprietary tools and research to identify holdings that can be considered sustainable, either because these assets align with our classification of a sustainably managed entity or because the products, services, or representative collateral aligns with the sustainable themes outlined above. """,
                html.B("2 Other securities"),
                """ represent securities that are not evaluated for the purposes of sustainable characteristics. These securities are allowed up to 20% of the portfolio and include cash, cash equivalents, or other instruments that are used for the purposes of portfolio liquidity and hedging only. Portfolio market value is calculated on a trade date basis. A negative market value represents forward settling trades (specifically agency MBS TBAs) that are backed by liquid securities other than cash. """,
                html.B("3 Weighted Average Carbon Intensity measure"),
                """ represents the weighted average summary of the portfolio company’s most recently reported or estimated Scope 1 and 2 emissions normalized by the most recently available sales in million USD. """,
                html.B("4 Carbon intensity reduction"),
                """ relative to benchmark and/or universe. Applies to equity securities. """,
                html.B("5 TCW has developed a scoring methodology"),
                """ to assess the ESG risk or characteristics of companies and/or issuers. All securities are assessed according to this methodology. """,
                html.B("6 Not scored securities"),
                """ represent securities that are not evaluated for the purposes of sustainable characteristics. These include cash, cash equivalents, or other instruments that are used for the purposes of portfolio liquidity and hedging only. Portfolio market value is calculated on a trade date basis. A negative market value represents forward settling trades (specifically agency MBS TBAs) that are backed by liquid securities other than cash.""",
                html.Br(className="m"),
                html.Span(
                    [
                        html.B(index_text[0] + ":"),
                        index_text[1],
                    ],
                ),
            ]
        else:
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
                html.Br(),
            ]
        return super().add_footnote(footnote_text)

    def add_body_equity_fi(self) -> html.Div:
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
                    # self.add_transition_table(),
                ],
                className="five columns",
            ),
            # third column
            html.Div(
                [self.add_carbon_intensity_chart()],
                className="four columns",
            ),
        ]
        return body_content

    def add_body_equity_a9(self) -> html.Div:
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
                    self.add_summary_table(),
                    self.add_WACI_table(),
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
                    # self.add_transition_table(),
                ],
                className="five columns",
            ),
            # third column
            html.Div(
                [self.add_carbon_intensity_chart()],
                className="four columns",
            ),
        ]
        return body_content

    def add_body_fi_a9(self) -> html.Div:
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
                    self.add_summary_table(),
                    self.add_WACI_table(),
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
                    # self.add_transition_table(),
                ],
                className="five columns",
            ),
            # third column
            html.Div(
                [self.add_carbon_intensity_chart()],
                className="four columns",
            ),
        ]
        return body_content

    def add_body_em(self) -> html.Div:
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
                    self.add_summary_table(),
                    self.add_bond_table(),
                    self.add_WACI_table(),
                ],
                className="four columns",
            ),
            # second column
            html.Div(
                [
                    self.add_risk_score_distribution(),
                    self.add_sustainable_classification_donut(),
                ],
                className="four columns",
            ),
            # third column
            html.Div(
                [self.add_country_table(), self.add_sector_table()],
                className="four columns",
            ),
        ]
        return body_content

    def add_body_portfolio_holdings(self, page_no: int) -> html.Div:
        """
        Create main body with tables, charts and text

        Parameters
        ----------
        page_no: int
            page number

        Returns
        -------
        html.Div
            row with body Div
        """
        body_content = [
            html.Div(
                self.add_portfolio_table(page_no=page_no), className="twelve columns"
            )
        ]
        return super().add_body(body_content)

    def add_body(self) -> html.Div:
        """
        Create main body with tables, charts and text

        Returns
        -------
        html.Div
            row with body Div
        """
        if self.portfolio_type in ["fixed_income", "equity"]:
            body_content = self.add_body_equity_fi()
        elif self.portfolio_type in ["equity_a9"]:
            body_content = self.add_body_equity_a9()
        elif self.portfolio_type in ["fixed_income_a9", "fixed_income_a8"]:
            body_content = self.add_body_fi_a9()
        elif self.portfolio_type == "em":
            body_content = self.add_body_em()
        return super().add_body(body_content)

    def add_portfolio_table(self, page_no: int) -> html.Div:
        """
        Table with Portfolio holdings

        Parameters
        ----------
        page_no: int
            page number

        Returns
        -------
        html.Div
            div with holdings table
        """
        df = deepcopy(self.portfolio_data)
        df["Risk Score"] = (
            df["ESRM Score"] + df["Governance Score"] + df["Transition Score"]
        ) / 3
        df["Theme"] = df["SCLASS_Level4-P"].map(mapping_utils.sclass_4_mapping)
        df["Risk Score Overall"] = df["Risk_Score_Overall"].map(
            mapping_utils.risk_score_overall_mapping
        )
        df["style"] = np.where(
            df["SCLASS_Level1"] == "Preferred",
            "preferred-bg",
            np.where(
                df["SCLASS_Level1"] == "Excluded",
                "excluded-bg",
                np.where(df["SCLASS_Level3"] == "Transition", "transition-bg", np.nan),
            ),
        )
        styles = df[df["style"] != "nan"]["style"].squeeze().to_dict()
        styles = {k: [v] for k, v in styles.items()}

        if not self.filtered:
            cols = [
                "Security ISIN",
                "Issuer Name",
                "Portfolio Weight",
                "SCLASS_Level1",
                "SCLASS_Level3",
                "SCLASS_Level4-P",
                "Theme",
                "ESRM Score",
                "Governance Score",
                "Transition Score",
                "Risk Score",
                "Securitized Score",
                "Sovereign Score",
                "Muni Score",
                "Risk Score Overall",
                "CARBON_EMISSIONS_SCOPE_12_INTEN",
                "Analyst",
            ]
        else:
            cols = [
                "Security ISIN",
                "Issuer Name",
                "Portfolio Weight",
                "SCLASS_Level1",
                "SCLASS_Level3",
                "SCLASS_Level4-P",
                "Theme",
                "ESRM Score",
                "Governance Score",
                "Transition Score",
                "Risk Score",
                "Risk Score Overall",
                "CARBON_EMISSIONS_SCOPE_12_INTEN",
                "Analyst",
            ]

        df = df[cols]
        df["Portfolio Weight"] = df["Portfolio Weight"].apply(
            lambda x: "{:,.2f}".format(x)
        )
        df["Risk Score"] = df["Risk Score"].apply(lambda x: "{:,.2f}".format(x))
        df = df.rename(
            columns={
                "CARBON_EMISSIONS_SCOPE_12_INTEN": "Carbon Intensity Scope 12",
                "SCLASS_Level1": "SCLASS Level1",
                "SCLASS_Level3": "SCLASS Level3",
                "SCLASS_Level4-P": "SCLASS Level4",
                "ESRM Score": "E&S Score",
            }
        )
        df = df[
            page_no * self.portfolio_divider : (page_no + 1) * self.portfolio_divider
        ]

        holdings_div = html.Div(
            [
                self.add_table(
                    df,
                    show_header=True,
                    id=f"portfolio-table-{page_no}",
                    superscript=False,
                    styles=styles,
                ),
            ],
            className="row portfolio-table",
        )
        return holdings_div

    def add_summary_table(self) -> html.Div:
        """
        For portfolio, calculate summary measures.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with WACI table and header
        """
        scores = portfolio_utils.calculate_portfolio_summary(
            self.portfolio_data, self.portfolio_type
        )
        total = sum(scores.values())

        sm = (
            "E & S Characteristics"
            if self.portfolio_type == "fixed_income_a8"
            else "Sustainable Managed"
        )
        names = [
            sm,
            "Sustainable Themes",
            "Other2",
            "Total",
        ]

        values = [
            "{0:.2f}".format(scores["sustainable_managed"]),
            "{0:.2f}".format(scores["sustainable_theme"]),
            "{0:.2f}".format(scores["other"]),
            "{0:.2f}".format(total),
        ]
        styles = {2: ["italic"]}

        if scores["exclusion"] > 0:
            names.insert(2, "Exclusion")
            values.insert(2, "{0:.2f}".format(scores["exclusion"]))
            styles = {3: ["italic"]}

        df_summary = pd.DataFrame(
            data={
                "Name": names,
                "Value": values,
            }
        )

        summary_div = html.Div(
            [
                html.H6(
                    [
                        "Summary Characteristics",
                        html.Sup(1, className="superscript"),
                        html.A(" (%MV)", className="mv"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(df_summary, styles=styles),
            ],
            className="row",
            id="summary-table",
        )
        return summary_div

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
        waci_index = portfolio_utils.calculate_portfolio_waci(self.waci_benchmark_data)
        carbon_reduction = waci_portfolio / waci_index - 1 if waci_index > 0 else 0

        if not self.benchmark_data.empty:
            data = [
                "{0:.2f}".format(waci_portfolio),
                "{0:.2f}".format(waci_index),
                format(carbon_reduction, ".0%"),
            ]
        else:
            data = [
                "{0:.2f}".format(waci_portfolio),
                "-",
                "-",
            ]

        sub_fund = "Sub-Fund4" if self.portfolio_type == "em" else "Sub-Fund"
        cr_label = (
            "Carbon Reduction5" if self.portfolio_type == "em" else "Carbon Reduction4"
        )

        df_WACI = pd.DataFrame(
            data={
                "Name": [sub_fund, "Index", cr_label],
                "Value": data,
            }
        )

        sup = (
            3
            if self.portfolio_type
            in ["equity_a9", "fixed_income_a9", "em", "fixed_income_a8"]
            else 1
        )
        waci_div = html.Div(
            [
                html.H6(
                    [
                        "Weighted Average Carbon Intensity",
                        html.Sup(sup, className="superscript"),
                        html.Br(),
                        "- Tons CO",
                        html.Sub(2),
                        "e/$M Sales",
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
        styles = {}
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

        labels = ["Corp/Quasis", "E&S", "Governance", "Transition"]
        portfolio = [
            "{0:.2f}".format(total_portfolio),
            "{0:.2f}".format(esrm_portfolio),
            "{0:.2f}".format(gov_portfolio),
            "{0:.2f}".format(trans_portfolio),
        ]
        if not self.benchmark_data.empty:
            ind = [
                "{0:.2f}".format(total_index),
                "{0:.2f}".format(esrm_index),
                "{0:.2f}".format(gov_index),
                "{0:.2f}".format(trans_index),
            ]
        else:
            ind = ["-"] * 4

        if self.portfolio_type == "em":
            sov_portfolio = portfolio_utils.calculate_portfolio_sovereign(
                self.portfolio_data
            )
            sov_index = portfolio_utils.calculate_portfolio_sovereign(
                self.benchmark_data
            )

            labels.insert(0, "Sovereign")
            portfolio.insert(0, "{0:.2f}".format(sov_portfolio))
            ind.insert(0, "{0:.2f}".format(sov_index))

            styles = {0: ["grey-row"], 1: ["grey-row"], 4: ["normal-row"]}
        else:
            styles = {0: ["grey-row"], 3: ["normal-row"]}

        df_risk_score_distr = pd.DataFrame(
            data={
                "": labels,
                "Portfolio": portfolio,
                "Index": ind,
            }
        )

        distr_div = html.Div(
            [
                html.H6(
                    [
                        "TCW ESG Risk Score ",
                        html.A("(1 (leader) to 5 (laggard))", className="mv"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(
                    df_risk_score_distr,
                    show_header=True,
                    id="risk-score",
                    styles=styles,
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
                    "Not Scored6",
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

        styles = {3: ["italic"]}

        distr = html.Div(
            [
                html.H6(
                    [
                        "ESG Risk Score Distribution",
                        html.Sup(5, className="superscript"),
                        html.A(" (%MV)", className="mv"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(df_distr, styles=styles),
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
        styles = dict()
        scores = portfolio_utils.calculate_planet_distribution(self.portfolio_data)
        sust_total = sum(scores.values())

        names = [
            "Renewable Energy, Storage, and Green Hydrogen",
            "Sustainable Mobility",
            "Circular Economy",
            "Climate Change Adaption & Risk Management",
            "Biodiversity & Sustainable Land & Water Use",
            "Sustainable Real Assets & Smart Cities",
            "Total Planet Themes",
        ]
        values = [
            "{0:.2f}".format(scores["RENEWENERGY"]),
            "{0:.2f}".format(scores["MOBILITY"]),
            "{0:.2f}".format(scores["CIRCULARITY"]),
            "{0:.2f}".format(scores["CCADAPT"]),
            "{0:.2f}".format(scores["BIODIVERSITY"]),
            "{0:.2f}".format(scores["SMARTCITIES"]),
            "{0:.2f}".format(sust_total),
        ]

        if self.portfolio_type in [
            "fixed_income",
            "fixed_income_a9",
            "fixed_income_a8",
        ]:
            bonds = portfolio_utils.calculate_bond_distribution(self.portfolio_data)
            names.insert(-1, "Green - Labeled Bonds")
            values.insert(-1, "{0:.2f}".format(bonds["Labeled Green"]))
            sust_total += bonds["Labeled Green"]
            values[-1] = "{0:.2f}".format(sust_total)
            styles = {6: ["italic"]}

        planet_table = pd.DataFrame(
            data={
                "Name": names,
                "Value": values,
            }
        )

        t = self.add_table(
            planet_table, add_vertical_column="Planet", id="planet-table", styles=styles
        )

        sust_table = html.Div(
            [
                html.H6(
                    [
                        "Detailed Highlights - Sustainable Themes ",
                        html.A("(%MV)", className="mv"),
                    ],
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
        styles = dict()
        scores = portfolio_utils.calculate_people_distribution(self.portfolio_data)
        sust_total = sum(scores.values())

        names = [
            "Health",
            "Sanitation and Hygiene",
            "Education",
            "Financial and Digital Inclusion",
            "Nutrition",
            "Affordable and Inclusive Housing",
            "Total People Themes",
        ]
        values = [
            "{0:.2f}".format(scores["HEALTH"]),
            "{0:.2f}".format(scores["SANITATION"]),
            "{0:.2f}".format(scores["EDU"]),
            "{0:.2f}".format(scores["INCLUSION"]),
            "{0:.2f}".format(scores["NUTRITION"]),
            "{0:.2f}".format(scores["AFFORDABLE"]),
            "{0:.2f}".format(sust_total),
        ]

        if self.portfolio_type in [
            "fixed_income",
            "fixed_income_a9",
            "fixed_income_a8",
        ]:
            bonds = portfolio_utils.calculate_bond_distribution(self.portfolio_data)
            names.insert(-1, "Social - Labeled Bonds")
            values.insert(-1, "{0:.2f}".format(bonds["Labeled Social"]))
            sust_total += bonds["Labeled Social"]
            values[-1] = "{0:.2f}".format(sust_total)
            styles = {6: ["italic"]}

        people_table = pd.DataFrame(
            data={
                "Name": names,
                "Value": values,
            }
        )

        t = self.add_table(
            people_table, add_vertical_column="People", id="people-table", styles=styles
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
        styles = dict()
        scores_people = portfolio_utils.calculate_people_distribution(
            self.portfolio_data
        )
        scores_planet = portfolio_utils.calculate_planet_distribution(
            self.portfolio_data
        )
        total = sum(scores_people.values()) + sum(scores_planet.values())

        names = ["Total Sustainable Themes"]
        values = ["{0:.2f}".format(total)]

        if self.portfolio_type in [
            "fixed_income",
            "fixed_income_a9",
            "fixed_income_a8",
        ]:
            bonds = portfolio_utils.calculate_bond_distribution(self.portfolio_data)
            total += sum(bonds.values())
            values[-1] = "{0:.2f}".format(total)
            names.insert(-1, "Sustainability Bonds")
            names.insert(-1, "Sustainability-Linked Bonds")
            values.insert(-1, "{0:.2f}".format(bonds["Labeled Sustainable"]))
            values.insert(-1, "{0:.2f}".format(bonds["Labeled Sustainable Linked"]))
            styles = {0: ["italic"], 1: ["italic"]}

        total_table = pd.DataFrame(data={"Name": names, "Value": values})

        t = self.add_table(
            total_table, add_vertical_column=" ", id="total-table", styles=styles
        )

        sust_table = html.Div(
            [t],
            className="row",
        )
        return sust_table

    def add_bond_table(self) -> html.Div:
        """
        For a portfolio, calculate the total portfolio weight for sustainable themes
        Show these values in a table.

        Returns
        -------
        html.Div
            div with total score table
        """
        styles = {0: ["bold", "size13"]}
        bonds = portfolio_utils.calculate_bond_distribution(self.portfolio_data)
        total = sum(bonds.values())

        names = [
            "Labeled Bonds",
            "Green",
            "Social",
            "Sustainability",
            "Sustainability-Linked",
            "Green/Sustainability-Linked",
        ]
        values = [
            "{0:.2f}".format(total),
            "{0:.2f}".format(bonds["Labeled Green"]),
            "{0:.2f}".format(bonds["Labeled Social"]),
            "{0:.2f}".format(bonds["Labeled Sustainable"]),
            "{0:.2f}".format(bonds["Labeled Sustainable Linked"]),
            "{0:.2f}".format(bonds["Labeled Green/Sustainable Linked"]),
        ]

        bond_table = pd.DataFrame(data={"Name": names, "Value": values})

        bond_div = html.Div(
            [
                html.H6(
                    ["ESG Bond by Type ", html.A("(% MV)", className="mv")],
                    className="subtitle padded",
                ),
                self.add_table(bond_table, styles=styles),
            ],
            className="row",
            id="bond-table",
        )
        return bond_div

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

    def add_country_table(self) -> html.Div:
        """
        For portfolio and benchmark, calculate the WACI and carbon reduction.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with WACI table and header
        """
        country = portfolio_utils.calculate_country_distribution(self.portfolio_data)
        country["Contribution"] = country["Contribution"] * 100
        country["Contribution"] = (
            country["Contribution"].astype(float).map("{:.2f}".format)
        )

        country_div = html.Div(
            [
                html.H6(
                    [
                        "Labeled Bonds by Country ",
                        html.A("(%labeled Bonds)", className="mv"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(country),
            ],
            className="row",
            id="country-table",
        )
        return country_div

    def add_sector_table(self) -> html.Div:
        """
        For portfolio and benchmark, calculate the WACI and carbon reduction.
        Show these values in a table.

        Returns
        -------
        html.Div
            div with WACI table and header
        """
        sector = portfolio_utils.calculate_sector_distribution(self.portfolio_data)
        sector["Contribution"] = sector["Contribution"] * 100
        sector["Contribution"] = (
            sector["Contribution"].astype(float).map("{:.2f}".format)
        )

        sector_div = html.Div(
            [
                html.H6(
                    [
                        "Labeled Bonds by Sector ",
                        html.A("(%labeled Bonds)", className="mv"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(sector),
            ],
            className="row",
            id="sector-table",
        )
        return sector_div

    def add_carbon_intensity_chart(self) -> html.Div:
        """
        For a portfolio, calculate carbon intensity per sector.
        Show these values in a bar chart.

        Returns
        -------
        html.Div
            div with bar chart and header
        """
        df_ci = portfolio_utils.calculate_carbon_intensity(
            self.portfolio_data, self.portfolio_type
        )
        df_ci["Sector"] = df_ci["Sector"].str.replace(" ", "   <br>")
        df_ci["Sector"] = df_ci["Sector"].str.replace(
            "Government   <br>Owned", "Government Owned"
        )
        df_ci["Sector"] = df_ci["Sector"].str.replace(
            "No   <br>Guarantee", "No Guarantee"
        )
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

    def add_sustainable_classification_donut(self) -> html.Div:
        """
        For a portfolio, sustainable classification distribution.
        Show these values in a bar chart.

        Returns
        -------
        html.Div
            div with donut chart and header
        """
        df = portfolio_utils.calculate_sustainable_classification(self.portfolio_data)
        table = []
        legend_row = []
        for index, row in df.iterrows():
            label = row["SCLASS_Level2"]
            color = row["Color"][1:]
            if index % 2 == 0:
                legend_row.append(
                    html.Td(
                        [html.Div(className=f"square bg-{color}"), html.Span([label])]
                    )
                )
                if index == len(df) - 1:
                    table.append(html.Tr(legend_row))
            else:
                legend_row.append(
                    html.Td(
                        [html.Div(className=f"square bg-{color}"), html.Span([label])]
                    )
                )
                table.append(html.Tr(legend_row))
                legend_row = []
        legend = html.Table(table, id="donut-legend")

        chart = html.Div(
            [
                html.Div(
                    [
                        html.H6(
                            [
                                "TCW Sustainable Classification ",
                                html.A("(%MV)", className="mv"),
                            ],
                            className="subtitle padded",
                        ),
                    ]
                ),
                self.add_pie_chart(
                    labels=df["SCLASS_Level2"],
                    values=df["Portfolio Weight"],
                    color=df["Color"],
                    hole=0.5,
                    width=230,
                    height=230,
                ),
                legend,
            ],
            className="row sust-pie",
        )
        return chart
