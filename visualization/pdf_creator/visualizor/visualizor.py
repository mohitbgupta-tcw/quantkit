import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
from utils import make_dash_table
import plotly.graph_objs as go
import pathlib
import pandas as pd

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../data").resolve()


df_price_perf = pd.read_csv(DATA_PATH.joinpath("df_price_perf.csv"))

df_WACI = pd.DataFrame(
    data={"Name": ["Portfolio", "Index", "Carbon Reduction"], "Value": [50, 100, -50]}
)
df_risk_score_distr = pd.DataFrame(
    data={
        "": ["E&S", "Governance", "Transition", "Overall"],
        "Portfolio": [2, 2, 2, 2],
        "Index": [3, 3, 3, 3],
    }
)
df_distr = pd.DataFrame(
    data={
        "Name": [
            "Leading ESG Score",
            "Average ESG Score",
            "Poor Risk Score",
            "Not Scored",
            "Total",
        ],
        "Value": [30, 50, 10, 10, 100],
    }
)
df_ci_sector = pd.DataFrame(
    data={
        "Name": [
            "Utilities",
            "Unassigned <br> GICS",
            "Energy",
            "Materials",
            "Industrials",
            "Real <br> Estate",
            "Consumer <br> Staples",
            "Consumer <br> Discretionary",
            "Communication <br> Services",
            "Information <br> Technology",
        ],
        "Value": [1706, 1219, 527, 250, 224, 70, 36, 31, 27, 19],
    }
)
df_ci_sector = df_ci_sector.sort_values(["Value"], ascending=True)


class Visualizor(object):
    def __init__(self, title):
        self.app = dash.Dash(
            __name__,
            meta_tags=[{"name": "viewport", "content": "width=device-width"}],
        )
        self.app.title = title
        self.server = self.app.server

        # Describe the layout/ UI of the app
        self.app.layout = self.create_layout()

    def add_header(self) -> html.Div:
        header = html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.H5(
                                    "Sustainable Characteristics", id="overall-header"
                                ),
                                html.H6("Portfolio:"),
                                html.H6("Benchmark:"),
                                html.H6("As of: 6/30/2023"),
                            ],
                            className="seven columns main-title",
                        ),
                        html.Div(
                            [
                                html.Img(
                                    src=self.app.get_asset_url("tcw_logo.png"),
                                    className="tcw-logo",
                                )
                            ],
                            className="five columns",
                            id="header-logo",
                        ),
                    ],
                    className="twelve columns",
                    style={"padding-left": "0"},
                ),
            ],
            className="row",
        )
        return header

    def add_bar_chart(self, x, y) -> dcc.Graph:
        bar = dcc.Graph(
            figure={
                "data": [
                    go.Bar(
                        x=x,
                        y=y,
                        orientation="h",
                        text=x,
                        textposition="outside",
                    )
                ],
                "layout": go.Layout(
                    autosize=True,
                    title="",
                    font={"family": "Calibri", "size": 11},
                    height=350,
                    # width=400,
                    hovermode="closest",
                    margin={
                        "r": 20,
                        "t": 20,
                        "b": 20,
                        "l": 50,
                    },
                    showlegend=False,
                    yaxis={
                        "automargin": True,
                    },
                    xaxis={
                        "range": [0, max(x) + 200],
                        "automargin": False,
                        "showgrid": True,
                        "gridcolor": "lightgrey",
                        "griddash": "dot",
                    },
                ),
            },
            config={"displayModeBar": False},
        )
        return bar

    def add_table(self, df, multi_column=False, header=False) -> html.Table:
        """Return a dash definition of an HTML table for a Pandas dataframe"""
        table = []
        if header:
            header_row = []
            for i, col in enumerate(df.columns):
                if i == 0:
                    header_row.append(html.Th(col))
                else:
                    header_row.append(html.Th(col, className="vertical"))
            table.append(html.Tr(header_row, className="table-header"))

        if multi_column:
            class_td = "table-element vertical"
            class_table = "multi-table"
        else:
            class_td = "table-element"
            class_table = ""

        for index, row in df.iterrows():
            html_row = []
            for i in range(len(row)):
                if i == 0:
                    html_row.append(html.Td([row[i]]))
                else:
                    html_row.append(html.Td([row[i]], className=class_td))
            table.append(html.Tr(html_row))
        return html.Table(table, className=class_table)

    def add_WACI_table(self) -> html.Div:
        waci = html.Div(
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
        )
        return waci

    def add_risk_score_distribution(self) -> html.Div:
        distr = html.Div(
            [
                html.H6(
                    [
                        "TCW ESG Risk Score Distribution",
                        html.Sup(2, className="superscript"),
                    ],
                    className="subtitle padded",
                ),
                self.add_table(df_risk_score_distr, multi_column=True, header=True),
            ],
            className="row",
        )
        return distr

    def add_score_distribution(self) -> html.Div:
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
        )
        return distr

    def add_carbon_intensity_chart(self) -> html.Div:
        ci_chart = html.Div(
            [
                html.H6(
                    "Carbon Intensity by Sector",
                    className="subtitle padded",
                ),
                self.add_bar_chart(x=df_ci_sector["Value"], y=df_ci_sector["Name"]),
            ],
            className="row",
        )
        return ci_chart

    def add_equity_body(self) -> html.Div:
        body = html.Div(
            [
                # first columns
                html.Div(
                    [
                        self.add_WACI_table(),
                        self.add_risk_score_distribution(),
                        self.add_score_distribution(),
                    ],
                    className="three columns",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H6(
                                    "Hypothetical growth of $10,000",
                                    className="subtitle padded",
                                ),
                                dcc.Graph(
                                    id="graph-2",
                                    figure={
                                        "data": [
                                            go.Scatter(
                                                x=[
                                                    "2008",
                                                    "2009",
                                                    "2010",
                                                    "2011",
                                                    "2012",
                                                    "2013",
                                                    "2014",
                                                    "2015",
                                                    "2016",
                                                    "2017",
                                                    "2018",
                                                ],
                                                y=[
                                                    "10000",
                                                    "7500",
                                                    "9000",
                                                    "10000",
                                                    "10500",
                                                    "11000",
                                                    "14000",
                                                    "18000",
                                                    "19000",
                                                    "20500",
                                                    "24000",
                                                ],
                                                line={"color": "#97151c"},
                                                mode="lines",
                                                name="Calibre Index Fund Inv",
                                            )
                                        ],
                                        "layout": go.Layout(
                                            autosize=True,
                                            title="",
                                            font={"family": "Raleway", "size": 10},
                                            height=200,
                                            width=400,
                                            hovermode="closest",
                                            legend={
                                                "x": -0.0277108433735,
                                                "y": -0.142606516291,
                                                "orientation": "h",
                                            },
                                            margin={
                                                "r": 20,
                                                "t": 20,
                                                "b": 20,
                                                "l": 50,
                                            },
                                            showlegend=True,
                                            xaxis={
                                                "autorange": True,
                                                "linecolor": "rgb(0, 0, 0)",
                                                "linewidth": 1,
                                                "range": [2008, 2018],
                                                "showgrid": False,
                                                "showline": True,
                                                "title": "",
                                                "type": "linear",
                                            },
                                            yaxis={
                                                "autorange": False,
                                                "gridcolor": "rgba(127, 127, 127, 0.2)",
                                                "mirror": False,
                                                "nticks": 4,
                                                "range": [0, 30000],
                                                "showgrid": True,
                                                "showline": True,
                                                "ticklen": 10,
                                                "ticks": "outside",
                                                "title": "$",
                                                "type": "linear",
                                                "zeroline": False,
                                                "zerolinewidth": 4,
                                            },
                                        ),
                                    },
                                    config={"displayModeBar": False},
                                ),
                            ],
                            className="row",
                        ),
                    ],
                    className="five columns",
                ),
                html.Div(
                    [self.add_carbon_intensity_chart()],
                    className="four columns",
                ),
            ],
            className="sub_page twelve columns",
        )
        return body

    def create_layout(self) -> html.Div:
        # Page layouts
        layout = html.Div(
            [
                dcc.Location(id="url", refresh=False),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [self.add_header()],
                                    className="header",
                                ),
                                html.Div([self.add_equity_body()]),
                            ],
                            className="page",
                        )
                    ],
                    id="page-content",
                ),
            ]
        )

        return layout
