import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import pandas as pd

import seaborn as sns

sns.set()
sns.set_style()
sns.despine()
my_palette = [
    "#2138ab",
    "#8f99fb",
    "#3c73a8",
    "#4b57db",
    "#8ab8fe",
    "#9dbcd4",
    "#ada587",
    "#b59410",
    "#e2ca76",
    "#645403",
    "#af6f09",
    "#c9ae74",
    "#6f7632",
    "#154406",
    "#598556",
    "#536267",
    "#d6b4fc",
    "#703be7",
    "#856798",
    "#9e43a2",
]
sns.set_palette(my_palette)


def strategy_returns(
    strategy_df: pd.DataFrame, excluded_allocations: list = []
) -> None:
    """
    For a given DataFrame of returns, plot the line chart of cumulative returns

    Parameters
    ----------
    strategy_df: pd.DataFrame
        DataFrame with portfolio_name and daily returns
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

    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    plt.legend()
