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


df_fund_facts = pd.read_csv(DATA_PATH.joinpath("df_fund_facts.csv"))
df_price_perf = pd.read_csv(DATA_PATH.joinpath("df_price_perf.csv"))


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

    def add_equity_body(self) -> html.Div:
        body = html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.H6(["Fund Facts"], className="subtitle padded"),
                                html.Table(make_dash_table(df_fund_facts)),
                            ],
                            className="row",
                        ),
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
                    [
                        html.Div(
                            [
                                html.H6(
                                    "Price & Performance (%)",
                                    className="subtitle padded",
                                ),
                                html.Table(make_dash_table(df_price_perf)),
                            ],
                            className="row",
                        ),
                    ],
                    className="four columns",
                ),
            ],
            className="sub_page",
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
