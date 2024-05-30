import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import pandas as pd

import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px

sns.set()
sns.set_style()
sns.despine()
my_palette = [
    "#164863",
    "#706233",
    "#94A684",
    "#756AB6",
    "#427D9D",
    "#B0926A",
    "#9BBEC8",
    "#AC87C5",
    "#E1C78F",
    "#DDF2FD",
    "#FAE7C9",
    "#FFEEF4",
    "#FFE5E5",
]
sns.set_palette(my_palette)


def strategy_returns(
    strategy_df: pd.DataFrame,
    benchmark: pd.DataFrame = pd.DataFrame(),
    excluded_allocations: list = [],
) -> None:
    """
    For a given DataFrame of returns, plot the line chart of cumulative returns

    Parameters
    ----------
    strategy_df: pd.DataFrame
        DataFrame with portfolio_name and daily returns
    benchmark: pd.DataFrame, optional
        Benchmark daily returns
    excluded_allocations: list, optional
        list of startegies that should not be shown in graph
    """
    strategy_df = strategy_df.dropna()
    strategy_df["return"] = strategy_df["return"] + 1

    grouped = strategy_df.groupby("portfolio_name")

    fig, ax = plt.subplots(figsize=(8, 6))
    for name, group in grouped:
        if name in excluded_allocations:
            continue
        grouped_df = group["return"].cumprod()
        grouped_df.plot(ax=ax, label=name)

    if not benchmark.empty:
        b_label = benchmark["portfolio_name"].max()
        benchmark = (benchmark["return"] + 1).cumprod()
        benchmark.plot(ax=ax, label=b_label, color="#F37E52")

    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    plt.legend()
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))


def spider_plot(
    factor_df: pd.DataFrame,
    line_df: pd.DataFrame = None,
    header: str = "",
    palette: list = None,
) -> None:
    """
    Overlay spider splot with polar line chart

    Parameters
    ----------
    factor_df: pd.DataFrame
        DataFrame with quantile segment size in %, Format: Quantile | Fund | quantile_loc
    line_df: pd.DataFrame, optional
        DataFrame to plot line on top of segments, Format: Fund (index) || Plotting_Loc | Return_fmt
            Plotting_Loc indicates location in %
            Return_fmt indicates text to show next to datapoint
    header: str, optional
        plot header
    palette: list, optional
        color palette
    """
    if not palette:
        palette = [
            "rgb(248, 105, 107, 0.1)",
            "rgb(252, 163, 119, 0.1)",
            "rgb(255, 221, 130, 0.1)",
            "rgb(203, 220, 129, 0.1)",
            "rgb(133, 200, 125, 0.1)",
        ]

    fig1 = px.bar_polar(
        factor_df,
        r="quantile_loc",
        theta="Fund",
        color="Quantile",
        color_discrete_sequence=palette,
        width=1000,
        height=700,
    )

    if line_df is not None:
        fig2 = px.line_polar(
            line_df,
            r="Plotting_Loc",
            theta=line_df.index,
            line_close=True,
            range_r=(0, 1),
            template="plotly_dark",
            markers=True,
            text="Return_fmt",
        )
        fig2.update_traces(
            line=dict(width=2.5), marker={"size": 7.5}, textposition="top center"
        )

        fig = go.Figure(data=fig1.data + fig2.data)

    else:
        fig = go.Figure(data=fig1.data)

    fig.update_layout(
        title={
            "text": f"<b> {header}",
            "y": 0.925,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
        },
        polar=dict(radialaxis=dict(showticklabels=False, ticks="", linewidth=0)),
        showlegend=False,
    )

    fig.show()
