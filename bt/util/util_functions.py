def is_zero(x: float, tolerance: float = 1e-16) -> bool:
    """
    Test for zero that is robust against floating point precision errors

    Parameters
    ----------
    x: float
        float number
    tolerance: float, optional
        tolerance for zero check

    Returns
    -------
    bool:
        floot is zero
    """
    return abs(x) < tolerance
