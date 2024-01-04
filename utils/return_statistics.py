import pandas as pd
import numpy as np
from scipy.stats import norm as norm
import quantkit.utils.annualize_adjustments as annualize_adjustments
import quantkit.utils.mapping_configs as mapping_configs


def total_return(return_series: pd.DataFrame) -> float:
    """
    Calculate total return of return series

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy

    Returns
    -------
    float
        total return
    """
    return_series = return_series.dropna()
    return_series["return"] = return_series["return"] + 1
    return return_series["return"].cumprod()[-1] - 1


def ytd_return(return_series: pd.DataFrame) -> float:
    """
    Calculate YTD return of return series

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy

    Returns
    -------
    float
        ytd return
    """
    return_series = return_series.dropna()
    return_series["return"] = return_series["return"] + 1
    max_year = return_series.index.max().year
    return (
        return_series[return_series.index.year == max_year]["return"].cumprod()[-1] - 1
    )


def mean_return(
    return_series: pd.DataFrame, annualized: bool = False, frequency: str = "DAY"
) -> float:
    """
    Calculate mean return of return series

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    annualized: bool, optional
        annualize return
    frequency: str, optional
        frequency of data

    Returns
    -------
    float
        mean (annualized) return
    """
    return_series = return_series.dropna()
    m = return_series["return"].mean()
    if annualized:
        m = annualize_adjustments.compound_annualization(
            m, mapping_configs.annualize_factor_d[frequency]
        )
    return m


def n_year_return(
    return_series: pd.DataFrame, n: float, frequency: str = "DAY"
) -> float:
    """
    Calculate return of n years of return series

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    n: float
        number of years
    frequency: str, optional
        frequency of data

    Returns
    -------
    float
        n year total return
    """
    return_series = return_series.dropna()
    return_series["return"] = return_series["return"] + 1
    return (
        return_series.tail(mapping_configs.annualize_factor_d[frequency] * n)[
            "return"
        ].cumprod()[-1]
        - 1
    )


def volatility(
    return_series: pd.DataFrame, annualized: bool = False, frequency: str = "DAY"
) -> float:
    """
    Calculate volatility of return series

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    annualized: bool, optional
        annualize volatility
    frequency: str, optional
        frequency of data

    Returns
    -------
    float
        (annualized) volatility
    """
    return_series = return_series.dropna()
    vol = return_series["return"].std(ddof=1)
    if annualized:
        vol = annualize_adjustments.volatility_annualize(
            vol, mapping_configs.annualize_factor_d[frequency]
        )
    return vol


def sharpe(
    return_series: pd.DataFrame, annualized: bool = True, frequency: str = "DAY"
) -> float:
    """
    Calculate sharpe ratio of return series

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    annualized: bool, optional
        annualize volatility
    frequency: str, optional
        frequency of data

    Returns
    -------
    float
        sharpe ratio
    """
    return_series = return_series.dropna()
    m = mean_return(return_series=return_series)
    std = volatility(return_series=return_series)
    sharpe_ratio = m / std
    if annualized:
        sharpe_ratio = sharpe_ratio * np.sqrt(
            mapping_configs.annualize_factor_d[frequency]
        )
    return sharpe_ratio


def sortino(
    return_series: pd.DataFrame, annualized: bool = True, frequency: str = "DAY"
) -> float:
    """
    Calculate sortino ratio of return series

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    annualized: bool, optional
        annualize volatility
    frequency: str, optional
        frequency of data

    Returns
    -------
    float
        sortino ratio
    """
    return_series = return_series.dropna()
    m = mean_return(return_series=return_series)
    downside = np.sqrt(
        (return_series[return_series["return"] < 0]["return"] ** 2).sum()
        / len(return_series)
    )
    sortino_ratio = m / downside
    if annualized:
        sortino_ratio = sortino_ratio * np.sqrt(
            mapping_configs.annualize_factor_d[frequency]
        )
    return sortino_ratio


def value_at_risk(
    return_series: pd.DataFrame, sigma: int = 1, confidence: float = 0.95
):
    """
    Calculate daily value-at-risk
    (variance-covariance calculation with confidence n)

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    sigma: int, optional
        number of std deviations
    confidence: float, optional
        confidence

    Returns
    -------
    float
        VaR
    """
    return_series = return_series.dropna()
    m = mean_return(return_series=return_series)
    sigma *= volatility(return_series=return_series)

    return norm.ppf(1 - confidence, m, sigma)


def conditional_value_at_risk(
    return_series: pd.DataFrame, sigma: int = 1, confidence: float = 0.95
):
    """
    Calculate conditional daily value-at-risk (expected shortfall)
    -> quantifies the amount of tail risk of investment

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    sigma: int, optional
        number of std deviations
    confidence: float, optional
        confidence

    Returns
    -------
    float
        cVaR
    """
    return_series = return_series.dropna()
    var = value_at_risk(return_series, sigma, confidence)
    c_var = return_series[return_series["return"] < var]["return"].values.mean()
    return c_var if ~np.isnan(c_var) else var


def max_drawdown(return_series: pd.DataFrame):
    """
    Calculate the maximum drawdown

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy

    Returns
    -------
    float
        max drawdown
    """
    return_series = return_series.dropna()
    return_series["return"] = return_series["return"] + 1
    return_series["return"] = return_series["return"].cumprod()
    return (
        return_series["return"] / return_series["return"].expanding(min_periods=0).max()
    ).min() - 1


def return_stats(return_series: pd.DataFrame, frequency: str = "DAY") -> dict:
    """
    Calculate return statistics

    Parameters
    ----------
    return_series: pd.DataFrame
        return series of strategy
    frequency: str, optional
        frequency of data

    Returns
    -------
    dict
        statistics
    """
    stats_dict = dict()

    stats_dict["Portfolio"] = return_series["portfolio_name"].max()
    stats_dict["Total Return"] = format(
        total_return(return_series=return_series), ".2%"
    )
    stats_dict["CAGR%"] = format(
        mean_return(return_series=return_series, annualized=True, frequency=frequency),
        ".2%",
    )
    stats_dict["Return YTD"] = format(ytd_return(return_series=return_series), ".2%")
    stats_dict["Return 1 Year"] = format(
        n_year_return(return_series=return_series, n=1), ".2%"
    )
    stats_dict["Return 3 Years"] = format(
        n_year_return(return_series=return_series, n=3), ".2%"
    )
    stats_dict["Daily Std Dev"] = format(volatility(return_series=return_series), ".2%")
    stats_dict["Annualized Std Dev"] = format(
        volatility(return_series=return_series, annualized=True, frequency=frequency),
        ".2%",
    )
    stats_dict["Sharpe Ratio"] = round(sharpe(return_series=return_series), 3)
    stats_dict["Sortino Ratio"] = round(sortino(return_series=return_series), 3)
    stats_dict["VaR"] = format(value_at_risk(return_series=return_series), ".2%")
    stats_dict["cVaR"] = format(
        conditional_value_at_risk(return_series=return_series), ".2%"
    )
    stats_dict["Max Drawdown"] = format(
        max_drawdown(return_series=return_series), ".2%"
    )
    return stats_dict
