import dash
from dash import dcc
from dash import html
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Union
import quantkit.visualization.pdf_creator.plots.bar_chart as bar_chart
import quantkit.visualization.pdf_creator.plots.line_chart as line_chart
import quantkit.visualization.pdf_creator.plots.pie_chart as pie_chart
import quantkit.visualization.pdf_creator.plots.table as table


class PDFCreator(object):
    """
    Main Class to build pdf with custom visualizations

    Parameters
    ----------
    title: str
        title of app
    data: pd.DataFrame
        DataFrame with data to be displayed in pdf
    """

    def __init__(self, title: str, data: pd.DataFrame):
        self.app = dash.Dash(
            __name__,
            meta_tags=[{"name": "viewport", "content": "width=device-width"}],
        )
        self.app.title = title
        self.server = self.app.server
        self.data = data

    def run(self) -> None:
        """
        Run and create the app
        """
        self.app.layout = self.create_layout()

    def create_layout(self):
        """
        Create the layout and pages of the pdf
        """
        raise NotImplementedError()

    def add_header(self, header_text: list = [""]) -> html.Div:
        """
        Create the pdf header as top row including:
            - overall header
            - TCW logo in right top corner

        Parameters
        ----------
        header_text: list, optional
            list of header elements

        Returns
        -------
        html.Div
            row with header Div
        """
        header = html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            header_text,
                            className="twelve columns main-title",
                        ),
                    ],
                    className="twelve columns",
                ),
            ],
            className="row header",
        )
        return header

    def add_footnote(self, footnote_text: list = [""]) -> html.Div:
        """
        Create the pdf footer as bottom row including:
        - sources
        - disclosure information

        Parameters
        ----------
        footnote_text: list, optional
            list of header elements

        Returns
        -------
        html.Div
            row with footer Div
        """
        footnote = html.Div(
            [
                html.P(footnote_text, className="footer-textbox"),
                html.Div(
                    [
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
                        html.Div(
                            datetime.today().strftime("%m/%d/%Y"),
                            className="seven columns date",
                        ),
                    ],
                    className="twelve columns footnote-end",
                ),
            ],
            className="row footnote",
        )
        return footnote

    def add_body(self, body_content: list = [""]) -> html.Div:
        """
        Create main body div

        Parameters
        ----------
        body_content: list, optional
            list of body elements

        Returns
        -------
        html.Div
            row with body Div
        """
        body = html.Div(
            body_content,
            className="row sub_page twelve columns",
        )
        return body

    def create_page(
        self, header: html.Div, body: html.Div, footnote: html.Div, page_no: int
    ) -> html.Div:
        """
        Describe the layout/ UI of the app
        Add Header, Body, Footnote

        Parameters
        ----------
        header: html.Div
            header of page
        body: html.Div
            body of page
        footnote: html.Div
            footnote of page
        page_no: int
            page number

        Returns
        -------
        html.Div
            page div
        """
        layout = html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                # header row
                                header,
                                # body row
                                body,
                                # footnote row
                                footnote,
                            ],
                            className="page",
                            id=f"page-{page_no}",
                        )
                    ],
                ),
            ]
        )
        return layout

    def add_bar_chart(
        self,
        x: Union[list, np.array, pd.Series],
        y: Union[list, np.array, pd.Series],
        title: str = "",
        orientation: str = "h",
        height: int = 350,
        width: int = None,
    ) -> dcc.Graph:
        """
        Create bar chart from provided data

        Parameters
        ----------
        x: list | np.array | pd.Series
            data for x-axis
        y: list | np.array | pd.Series
            labels for y-axis
        title: str, optional
            Chart title
        orientation: str
            either "h" (horizontal) or "v" (vertically)
        height: int, optional
            height integer of chart in range [10, inf]
        width: int, optional
            width integer of chart in range [10, inf]

        Returns
        -------
        dcc.Graph
            bar chart
        """
        bar = bar_chart.BarChart(
            x, y, height=height, width=width, title=title, orientation=orientation
        )
        plot = bar.create_chart()
        return plot

    def add_line_chart(
        self,
        x: Union[list, np.array, pd.Series],
        y: Union[list, np.array, pd.Series],
        title: str = "",
        height: int = 350,
        width: int = None,
    ) -> dcc.Graph:
        """
        Create line chart from provided data

        Parameters
        ----------
        x: list | np.array | pd.Series
            data for x-axis
        y: list | np.array | pd.Series
            data for y-axis
        title: str, optional
            Chart title
        height: int, optional
            height integer of chart in range [10, inf]
        width: int, optional
            width integer of chart in range [10, inf]

        Returns
        -------
        dcc.Graph
            line chart
        """
        line = line_chart.LineChart(x, y, title=title, height=height, width=width)
        plot = line.create_chart()
        return plot

    def add_pie_chart(
        self,
        labels: Union[list, np.array, pd.Series],
        values: Union[list, np.array, pd.Series],
        color: Union[list, np.array, pd.Series],
        hole: float = 0,
        title: str = "",
        height: int = 350,
        width: int = None,
    ) -> dcc.Graph:
        """
        Create plotly pie chart

        Parameters
        ----------
        labels: list | np.array | pd.Series
            data labels
        values: list | np.array | pd.Series
            data values
        color: list | np.array | pd.Series
            color array for slizes
        hole: float, optional
            size of hole in middle
        title: str, optional
            Chart title
        height: int, optional
            height integer of chart in range [10, inf]
        width: int, optional
            width integer of chart in range [10, inf]

        Returns
        -------
        dcc.Graph
            donut chart
        """
        line = pie_chart.PieChart(
            values=values,
            labels=labels,
            color=color,
            hole=hole,
            title=title,
            height=height,
            width=width,
        )
        plot = line.create_chart()
        return plot

    def add_table(
        self,
        df: pd.DataFrame,
        id: str = "",
        show_vertical_lines: bool = False,
        show_header: bool = False,
        add_vertical_column: str = None,
        superscript: bool = True,
        styles: dict = {},
    ) -> html.Table:
        """
        Create a dash definition of an HTML table for a Pandas dataframe

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame to be displayed in table
        id: str, optional
            id of html table
        show_vertical_lines: bool, optional
            show vertical lines between columns
        show_header: bool, optional
            show table header
        add_vertical_column: str, optional
            text of vertical column to be added in right column
        superscript: bool
            add superscript to first column
        styles: dict, optional
            specify specific column for row in format:
                {
                    column_id: ["style1", "style2"]
            }

        Returns
        -------
        html.Table
            html table
        """
        t = table.Table(
            df,
            id=id,
            show_header=show_header,
            show_vertical_lines=show_vertical_lines,
            add_vertical_column=add_vertical_column,
            superscript=superscript,
            styles=styles,
        )
        plot = t.create_table()
        return plot
