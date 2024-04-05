def dflt_comm_fn(q: float, p: float) -> float:
    """
    Default commission function, assuming no transaction fees

    Parameters
    ----------
    q: float
        quantity bought
    p: float
        price

    Returns
    -------
    float:
        total transaction fee
    """
    return 0.0


def quantity_comm_fn(q: float, p: float) -> float:
    """
    Calcualte transaction fees based on quantity, with maximum set at $100

    Parameters
    ----------
    q: float
        quantity bought
    p: float
        price

    Returns
    -------
    float:
        total transaction fee
    """
    return max(100, abs(q) * 0.0021)
