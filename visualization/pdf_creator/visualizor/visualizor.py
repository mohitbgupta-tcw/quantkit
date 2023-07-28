import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output


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
                            ],
                            className="page",
                        )
                    ],
                    id="page-content",
                ),
            ]
        )

        return layout
