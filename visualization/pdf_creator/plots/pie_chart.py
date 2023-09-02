from typing import Union
import numpy as np
import pandas as pd
from dash import dcc
import plotly.graph_objs as go


class PieChart(object):
    """
    Class to create plotly pie chart

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
    """

    def __init__(
        self,
        labels: Union[list, np.array, pd.Series],
        values: Union[list, np.array, pd.Series],
        color: Union[list, np.array, pd.Series],
        hole: float = 0,
        title: str = "",
        height: int = 350,
        width: int = None,
    ):
        self.labels = labels
        self.values = values
        self.color = color
        self.hole = hole
        self.title = title
        self.height = height
        self.width = width

    def create_chart(
        self,
    ) -> dcc.Graph:
        """
        Create pie chart from provided data

        Returns
        -------
        dcc.Graph
            pie chart
        """
        pie = dcc.Graph(
            figure={
                "data": [
                    go.Pie(
                        labels=self.labels,
                        values=self.values,
                        hole=self.hole,
                        marker_colors=self.color,
                        sort=False,
                        texttemplate="%{value:.0f}%",
                        direction="clockwise",
                        showlegend=False,
                    )
                ],
                "layout": go.Layout(
                    autosize=True,
                    title=self.title,
                    font={"family": "Calibri", "size": 12},
                    height=self.height,
                    width=self.width,
                    hovermode="closest",
                    margin={
                        "r": 20,
                        "t": 20,
                        "b": 5,
                        "l": 20,
                    },
                    legend=dict(
                        yanchor="bottom",
                        y=-0.1,
                        xanchor="center",
                        x=0.5,
                        yref="container",
                        orientation="h",
                        tracegroupgap=1,
                    ),
                ),
            },
            config={"displayModeBar": False},
        )
        return pie
