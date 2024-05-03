import pandas as pd
import numpy as np


def group_mode(
    df: pd.DataFrame, group_by_column: str, mode_column: str
) -> pd.DataFrame:
    """
    For a DataFrame, find the mode of a column for each group of another column.

    Parameters
    ----------
    df: pd.DataFrame
        dataframe
    group_by_column: str
        column which indicates the final groups of the output
    mode_column: str
        column which we want to calculate the mode on

    Returns
    -------
    pd.DataFrame
        For each group, return one of the rows with the mode from the original DataFrame
    """
    # count the occurances of each item in the mode column per group
    df_ = df.groupby([group_by_column, mode_column])[mode_column].agg("count")

    # make Series a DataFrame and sort by count to have highest occurance first
    df_ = pd.DataFrame(df_).rename(columns={mode_column: "Count"})
    df_ = df_.reset_index()
    df_ = df_.sort_values("Count", ascending=False)

    # get the highest count
    df_ = (
        df_.groupby(group_by_column)
        .agg({"Count": "max", mode_column: "first"})
        .reset_index()
    )

    # filter for mode rows in original Dataframe and only keep one of those
    df = df.merge(df_, on=[group_by_column, mode_column], how="inner")
    df = df.drop_duplicates(subset=[group_by_column])
    df = df.drop("Count", axis=1)
    return df


def nanprodwrapper(a: np.ndarray, **kwargs) -> float:
    """
    Calculate nanprod of an array.
    If all values are nan, return nan.

    Parameters
    ----------
    a: np.array
        array

    Returns
    -------
    float
        nanprod of array
    """
    if np.isnan(a).all():
        return np.nan
    else:
        return np.nanprod(a, **kwargs)


def scale(val: float, src: tuple, dst: tuple) -> float:
    """
    Scale given value from scale of src to scale of dst

    Parameters
    ----------
    val: float
        value to scale
    src: tuple
        tuple of scale value is in
    dst: tuple
        tuple of scale value should be mapped to

    Returns
    -------
    float:
        rescaled value
    """
    return ((val - src[0]) / (src[1] - src[0])) * (dst[1] - dst[0]) + dst[0]


def float_to_percentage(col: pd.Series) -> pd.Series:
    """
    Format column of type float to column of type str
    in percentage format

    Parameters
    ----------
    col: pd.Series
        col of type float

    Returns
    -------
    pd.Series:
        col of type str in percentage format
    """
    return col.transform(lambda x: "{:,.2%}".format(x))
