import pandas as pd


def group_mode(df: pd.DataFrame, group_by_column: str, mode_column: str):
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
