import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import pandas as pd

import seaborn as sns

sns.set()
sns.set_style()
sns.despine()


def strategy_weights(
    allocation_df: pd.DataFrame, palette: list = None, title: str = None
) -> None:
    """
    For a given DataFrame of weights, plot the area chart

    Parameters
    ----------
    allocation_df: pd.DataFrame
        DataFrame with assets in columns, weights in rows
    palette: list, optional
        color palette
    title: str = None
        plot title
    """
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
    if palette:
        sns.set_palette(palette)
    else:
        sns.set_palette(my_palette)

    # Data
    x = list(allocation_df.index)
    y = allocation_df.transpose().values.tolist()

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    plt.stackplot(x, y, labels=allocation_df.columns)
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    plt.legend()
    plt.title(title)
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))


def weights_returns_overlay(
    allocation_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    palette: list = None,
    title: str = None,
    ax2_legend_loc: tuple = (1.095, -0.05),
) -> None:
    """
    For a given DataFrame of weights and returns,
    plot the area chart and overlay returns chart

    Parameters
    ----------
    allocation_df: pd.DataFrame
        DataFrame with assets in columns, weights in rows
    returns_df: pd.DataFrame
        returns of strategies to show
    palette: list, optional
        color palette
    title: str = None
        plot title
    ax2_legend_loc: tuple, optional
        location of ax2 legend
    """
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
    if palette:
        sns.set_palette(palette)
    else:
        sns.set_palette(my_palette)

    # Data
    x = list(returns_df.index)
    y = allocation_df.loc[returns_df.index].transpose().values.tolist()

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    ax2 = ax.twinx()

    ax.stackplot(x, y, labels=allocation_df.columns, alpha=0.7)

    colors = ["#000000", "#9a0200", "#0652ff", "#ffffe4"]
    ax2.set_prop_cycle(color=colors)
    ax2.plot(
        x,
        returns_df.cumprod(),
        label=returns_df.columns,
    )

    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax2.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax.set_ylabel("Weights")
    ax2.set_ylabel("Returns")
    ax2.grid(False)

    ax.legend([f"{i}: {allocation_df.iloc[-1][i]:.2%}" for i in allocation_df.columns])
    ax2.legend(loc=ax2_legend_loc)
    ax.margins(x=0, y=0)
    plt.title(title)
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1.09, 1))
