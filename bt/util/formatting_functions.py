import numpy as np


def fmtp(number: float) -> str:
    """
    Format decimal as percentage with 2 decimals

    Parameters
    ----------
    number: float
        decimal number

    Returns
    -------
    str:
        decimal formatted as percentage
    """
    if np.isnan(number):
        return "-"
    return format(number, ".2%")


def fmtn(number: float) -> str:
    """
    Format decimal to 2 decimals

    Parameters
    ----------
    number: float
        decimal number

    Returns
    -------
    str:
        decimal formatted
    """
    if np.isnan(number):
        return "-"
    return format(number, ".2f")


def fmtpn(number: float) -> str:
    """
    Format decimal as percentage with 2 decimals without % sign

    Parameters
    ----------
    number: float
        decimal number

    Returns
    -------
    str:
        decimal formatted as percentage
    """
    if np.isnan(number):
        return "-"
    return format(number * 100, ".2f")
