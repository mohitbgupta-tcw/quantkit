import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import pandas as pd

import seaborn as sns

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


def strategy_weights(allocation_df: pd.DataFrame) -> None:
    """
    For a given DataFrame of weights, plot the area chart

    Parameters
    ----------
    allocation_df: pd.DataFrame
        DataFrame with assets in columns, weights in rows
    """

    # Data
    x = list(allocation_df.index)
    y = allocation_df.transpose().values.tolist()

    # Plot
    fig, ax = plt.subplots(figsize=(8, 6))
    plt.stackplot(x, y, labels=allocation_df.columns)
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    plt.legend()
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
