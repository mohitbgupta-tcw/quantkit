from typing import Union
import numpy as np
import pandas as pd
from dash import dcc
import plotly.graph_objs as go


class LineChart(object):
    """
    Class to create plotly line chart

    Parameters
    ----------
    x: list | np.array | pd.Series
        data for x-axis
    y: list | np.array | pd.Series
        labels for y-axis
    title: str, optional
        Chart title
    height: int, optional
        height integer of chart in range [10, inf]
    width: int, optional
        width integer of chart in range [10, inf]
    """

    def __init__(
        self,
        x: Union[list, np.ndarray, pd.Series],
        y: Union[list, np.ndarray, pd.Series],
        title: str = "",
        height: int = 350,
        width: int = None,
    ) -> None:
        self.x = x
        self.y = y
        self.title = title
        self.height = height
        self.width = width

    def create_chart(
        self,
    ) -> dcc.Graph:
        """
        Create line chart from provided data

        Returns
        -------
        dcc.Graph
            line chart
        """
        line = dcc.Graph(
            figure={
                "data": [
                    go.Scatter(
                        x=self.x,
                        y=self.y,
                        line={"color": "#007DB5"},
                        mode="lines",
                    )
                ],
                "layout": go.Layout(
                    autosize=True,
                    title=self.title,
                    font={"family": "Raleway", "size": 10},
                    height=self.height,
                    width=self.width,
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
        )
        return line
