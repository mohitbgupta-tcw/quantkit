import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import pandas as pd


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
