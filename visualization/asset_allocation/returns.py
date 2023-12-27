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
        benchmark = (benchmark["return"] + 1).cumprod()
        benchmark.plot(ax=ax, label="benchmark", color="#F37E52")

    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    plt.legend()
