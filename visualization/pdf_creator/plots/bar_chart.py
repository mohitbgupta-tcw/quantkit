from typing import Union
import numpy as np
import pandas as pd
from dash import dcc
import plotly.graph_objs as go


class BarChart(object):
    """
    Class to create plotly bar chart

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
    """

    def __init__(
        self,
        x: Union[list, np.array, pd.Series],
        y: Union[list, np.array, pd.Series],
        title: str = "",
        orientation: str = "h",
        height: int = 350,
        width: int = None,
    ):
        self.x = x
        self.y = y
        self.title = title
        self.orientation = orientation
        self.height = height
        self.width = width

    def create_chart(
        self,
    ) -> dcc.Graph:
        """
        Create bar chart from provided data

        Returns
        -------
        dcc.Graph
            bar chart
        """
        bar = dcc.Graph(
            figure={
                "data": [
                    go.Bar(
                        x=self.x,
                        y=self.y,
                        orientation=self.orientation,
                        text=self.x,
                        textposition="outside",
                    )
                ],
                "layout": go.Layout(
                    autosize=True,
                    title=self.title,
                    font={"family": "Calibri", "size": 11},
                    height=self.height,
                    width=self.width,
                    hovermode="closest",
                    margin={
                        "r": 20,
                        "t": 20,
                        "b": 20,
                        "l": 50,
                    },
                    showlegend=False,
                    yaxis={"automargin": True, "ticksuffix": "   "},
                    xaxis={
                        "range": [0, max(self.x) + 400],
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
