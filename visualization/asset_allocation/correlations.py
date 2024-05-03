import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def correlations(corr_matrix: pd.DataFrame, assets: tuple, start_date: str) -> None:
    """
    Plot correlation of two assets over time

    Parameters
    ----------
    corr_matrix: pd.DataFrame
        correlation matrix over time with MultiIndex Data | Assets
    assets: tuple
        tuple of two assets to plot correlation for
    start_date: str
        year to start the plot on
    """
    corr_assets = (
        corr_matrix.loc[corr_matrix.index.get_level_values(1) == assets[0], assets[1]]
        .reset_index(level=1, drop=True)
        .loc[start_date:]
    )
    corr_pos = corr_assets.copy()
    corr_neg = corr_assets.copy()

    corr_pos[corr_pos <= 0] = np.nan
    corr_neg[corr_neg > 0] = np.nan

    corr_neg = corr_neg.fillna(0).where(corr_neg.shift(-1).notna())
    corr_pos = corr_pos.fillna(0).where(corr_pos.shift(1).notna())

    # plotting
    plt.plot(corr_pos, color="g")
    plt.plot(corr_neg, color="r")
    plt.ylim(-1, 1)
    plt.title(f"1 Year Correlation - {assets[0]} & {assets[1]}")
    plt.axhline(y=0.0, color="black", linestyle="-", linewidth=0.5)
    plt.show()
