from typing import Union
import pandas as pd
import numpy as np


def current_yield(
    price: Union[float, pd.DataFrame, np.ndarray],
    coupon: Union[float, pd.DataFrame, np.ndarray],
) -> pd.DataFrame:
    r"""
    Calculate Current Yield from price and annual coupon of bond

    Calculation
    -----------
    current_yield = \frac{coupon}{price}

    Parameters
    ----------
    price: float | pd.DataFrame
        price of bond(s)
    coupon: float | pd.DataFrame
        annual coupon of bond(s)

    Returns
    -------
    float | pd.DataFrame
        current yield
    """
    return coupon / price


def yield_to_maturity(
    face_value: Union[float, pd.DataFrame, np.ndarray],
    price: Union[float, pd.DataFrame, np.ndarray],
    time_to_maturity: Union[float, pd.DataFrame, np.ndarray],
    coupon: Union[float, pd.DataFrame, np.ndarray],
    timeline: pd.DatetimeIndex,
    names: list = None,
) -> pd.DataFrame:
    r"""
    Approximate calculation of Yield to Maturity from price
    Assumption: linear price change to face_value

    Calculation
    -----------
    ytm = \frac{annual_cashflow}{average_price}

    Parameters
    ----------
    face_value: float | pd.DataFrame
        face value of bond(s)
    price: float | pd.DataFrame
        price of bond(s)
    time_to_maturity: float | pd.DataFrame
        time to maturity of bond(s)
    coupon: float | pd.DataFrame
        annual coupon of bond(s)
    timeline: pd.DatetimeIndex
        time series
    names: list, optional
        column labels

    Returns
    -------
    float | pd.DataFrame
        yield to maturity
    """
    if isinstance(face_value, pd.DataFrame):
        face_value = face_value.to_numpy()
    if isinstance(price, pd.DataFrame):
        price = price.to_numpy()
    if isinstance(coupon, pd.DataFrame):
        coupon = coupon.to_numpy()
    if isinstance(time_to_maturity, pd.DataFrame):
        time_to_maturity = time_to_maturity.to_numpy()
    cash_flow = coupon + (face_value - price) / time_to_maturity
    average_price = (face_value + price) / 2.0
    current_yield = current_yield(average_price, cash_flow) * 100

    if names:
        return pd.DataFrame(current_yield, names=names, index=timeline)
    return pd.DataFrame(current_yield, index=timeline)


def yield_to_price(
    face_value: Union[float, pd.DataFrame, np.ndarray],
    bond_yield: Union[float, pd.DataFrame, np.ndarray],
    time_to_maturity: Union[float, pd.DataFrame, np.ndarray],
    coupon: Union[float, pd.DataFrame, np.ndarray],
    timeline: pd.DatetimeIndex,
    names: list = None,
) -> pd.DataFrame:
    r"""
    Approximate calculation of price from yield

    Parameters
    ----------
    face_value: float | pd.DataFrame
        face value of bond(s)
    bond_yield: float | pd.DataFrame
        price of bond(s)
    time_to_maturity: float | pd.DataFrame
        time to maturity of bond(s)
    coupon: float | pd.DataFrame
        annual coupon of bond(s)
    timeline: pd.DatetimeIndex
        time series
    names: list, optional
        column labels

    Returns
    -------
    float | pd.DataFrame
        price
    """
    if isinstance(face_value, pd.DataFrame):
        face_value = face_value.to_numpy()
    if isinstance(bond_yield, pd.DataFrame):
        bond_yield = bond_yield.to_numpy()
    if isinstance(coupon, pd.DataFrame):
        coupon = coupon.to_numpy()
    if isinstance(time_to_maturity, pd.DataFrame):
        time_to_maturity = time_to_maturity.to_numpy()

    price = (coupon + face_value / time_to_maturity - 0.5 * bond_yield) / (
        bond_yield / (2 * face_value) + 1 / time_to_maturity
    )

    if names:
        return pd.DataFrame(price, columns=names, index=timeline)
    return pd.DataFrame(price, index=timeline)


def pvbp(
    face_value: Union[float, pd.DataFrame, np.ndarray],
    bond_yield: Union[float, pd.DataFrame, np.ndarray],
    time_to_maturity: Union[float, pd.DataFrame, np.ndarray],
    coupon: Union[float, pd.DataFrame, np.ndarray],
    timeline: pd.DatetimeIndex,
    names: list = None,
) -> pd.DataFrame:
    """
    Price value of a basis point (PVBP) is a measure used to describe how a basis point change
    in yield affects the price of a bond.


    Parameters
    ----------
    face_value: float | pd.DataFrame
        face value of bond(s)
    bond_yield: float | pd.DataFrame
        price of bond(s)
    time_to_maturity: float | pd.DataFrame
        time to maturity of bond(s)
    coupon: float | pd.DataFrame
        annual coupon of bond(s)
    timeline: pd.DatetimeIndex
        time series
    names: list, optional
        column labels

    Returns
    -------
    float | pd.DataFrame
        pvbp
    """
    return yield_to_price(
        face_value, bond_yield + 0.01, time_to_maturity, coupon, timeline, names
    ) - yield_to_price(
        face_value, bond_yield, time_to_maturity, coupon, timeline, names
    )
