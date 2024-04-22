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
