from dash import html
from dash import dcc


def Header(app):
    return html.Div([get_header(app), html.Br([]), get_menu()])


def Header_only(app):
    return html.Div([get_header(app), html.Br([])])


def get_header(app):
    header = html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.H5("Sustainable Characteristics", id="overall-header"),
                            html.H6("Portfolio:"),
                            html.H6("Benchmark:"),
                            html.H6("As of: 6/30/2023"),
                        ],
                        className="seven columns main-title",
                    ),
                    html.Div(
                        [
                            html.Img(
                                src=app.get_asset_url("tcw_logo.png"),
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


def get_menu():
    menu = html.Div(
        [
            dcc.Link(
                "Overview",
                href="/dash-financial-report/overview",
                className="tab first",
            ),
            dcc.Link(
                "Price Performance",
                href="/dash-financial-report/price-performance",
                className="tab",
            ),
            dcc.Link(
                "Portfolio & Management",
                href="/dash-financial-report/portfolio-management",
                className="tab",
            ),
            dcc.Link(
                "Fees & Minimums", href="/dash-financial-report/fees", className="tab"
            ),
            dcc.Link(
                "Distributions",
                href="/dash-financial-report/distributions",
                className="tab",
            ),
            dcc.Link(
                "News & Reviews",
                href="/dash-financial-report/news-and-reviews",
                className="tab",
            ),
        ],
        className="row all-tabs",
    )
    return menu


def make_dash_table(df):
    """Return a dash definition of an HTML table for a Pandas dataframe"""
    table = []
    for index, row in df.iterrows():
        html_row = []
        for i in range(len(row)):
            html_row.append(html.Td([row[i]]))
        table.append(html.Tr(html_row))
    return table
