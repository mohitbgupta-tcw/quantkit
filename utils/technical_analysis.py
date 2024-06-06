import pandas as pd
import numpy as np
from scipy.signal import savgol_filter, find_peaks


def ADX(df: pd.DataFrame, lookback_periods: int = 14) -> pd.DataFrame:
    """
    Calculate ADX Trend Indicator, see https://www.investopedia.com/articles/trading/07/adx-trend-indicator.asp

    When the +DMI is above the -DMI, prices are moving up, and ADX measures the strength
    of the uptrend. When the -DMI is above the +DMI, prices are moving down, and ADX
    measures the strength of the downtrend.
    Many traders will use ADX readings above 25 to suggest that the trend is strong
    enough for trend-trading strategies. Conversely, when ADX is below 25, many will
    avoid trend-trading strategies.

    | ADX Value | Trend Strength         |
    | --------- | --------------         |
    | 0-25      | Absent or Weak Trend   |
    | 25-50     | Strong Trend           |
    | 50-75     | Very Strong Trend      |
    | 75-100    | Extremely Strong Trend |

    Parameters
    ----------
    df: pd.DataFrame:
        DataFrame with columns PX_HIGH, PX_LOW, PX_LAST
    lookback_periods: int, optional
        lookback window
    offset: int, optional
        offset for date shifting

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ADX, ADX_Trend
    """
    alpha = 1 / lookback_periods

    # +-DX
    df["+DX"] = df["PX_HIGH"] - df["PX_HIGH"].shift(1)
    df["-DX"] = df["PX_LOW"].shift(1) - df["PX_LOW"]

    df["+DX"] = np.where((df["+DX"] > df["-DX"]) & (df["+DX"] > 0), df["+DX"], 0.0)
    df["-DX"] = np.where((df["+DX"] < df["-DX"]) & (df["-DX"] > 0), df["-DX"], 0.0)

    # TR
    df["TR"] = np.maximum.reduce(
        [
            np.abs(df["PX_HIGH"] - df["PX_LAST"].shift(1)),
            np.abs(df["PX_LOW"] - df["PX_LAST"].shift(1)),
            df["PX_HIGH"] - df["PX_LOW"],
        ]
    )

    # ATR
    df["ATR"] = df["TR"].ewm(alpha=alpha, adjust=False).mean()

    # +- DMI
    df["+DMI"] = (df["+DX"].ewm(alpha=alpha, adjust=False).mean() / df["ATR"]) * 100
    df["-DMI"] = (df["-DX"].ewm(alpha=alpha, adjust=False).mean() / df["ATR"]) * 100

    # ADX
    df["DX"] = (np.abs(df["+DMI"] - df["-DMI"]) / (df["+DMI"] + df["-DMI"])) * 100
    df["ADX"] = df["DX"].ewm(alpha=alpha, adjust=False).mean()

    df["ADX_Trend"] = np.where(
        df["ADX"] < 25, 0, np.where(df["+DMI"] >= df["-DMI"], 1, -1)
    )

    del df["DX"], df["ATR"], df["TR"], df["-DX"], df["+DX"], df["+DMI"], df["-DMI"]
    return df


def weighted_average(
    data: pd.Series, lookback_periods: int, kernel_factor: int
) -> pd.Series:
    """
    Calculate weighted average based on polynomial kernel.
    The higher the kernel factor, the more forgetful the kernel, and
    therefore skewing to newer datapoints.

    Parameters
    ----------
    data: pd.Series
        Series of datapoints
    lookback_periods: int
        lookback periods
    kernel_factor: int
        kernel factor:
            - kernel_factor = 0 ==> uniform kernel
            - kernel_factor = 1 ==> linear kernel
            - kernel_factor > 1 ==> convex kernel
            - kernel_factor -> inf ==> all weight on the last observation

    Returns
    -------
    pd.Series
        rolling weighted average
    """
    kernel = np.array(
        [(lookback_periods - tt) ** kernel_factor for tt in range(0, lookback_periods)]
    )
    kernel = kernel / np.sum(kernel)
    kernel = kernel[::-1]

    return data.rolling(lookback_periods).apply(
        lambda seq: np.average(seq, weights=kernel)
    )
