import pandas as pd
from typing import Union, Tuple
import operator


def divide_chunks(l: list, n: int):
    """
    Divide a list into chunks of size n

    Parameters
    ----------
    l: list
        list to be divided
    n: int
        chunk size
    """
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i : i + n]


def iter_list(input_list: Union[list, set]) -> list:
    """
    Iterate over a list and save unique non-nan values

    Parameters
    ----------
    input_list: list | set
        list of elements

    Returns
    -------
    list
        unique values of input list which are not nan's
    """
    output_list = []
    for i in input_list:
        if not (pd.isna(i) or i in output_list):
            output_list.append(i)
    return output_list


def replace_dictionary(new_d: dict, original_d: dict) -> dict:
    """
    Recursively iterate over dictionary.
    Replace values in orignal dictionary by values from new dictionary.

    Parameters
    ----------
    new_d: dict
        dictionary with new values
    original_d: dict
        original dictionary

    Returns
    -------
    dict
        original dictionary with replaced values
    """
    if isinstance(new_d, dict):
        for k, v in new_d.items():
            if isinstance(new_d[k], (list, int, str, float)):
                original_d[k] = new_d[k]
            if k in original_d:
                replace_dictionary(v, original_d[k])
            else:
                original_d[k] = new_d[k]
    return original_d


def check_threshold(df: pd.DataFrame, msci_information: dict) -> Tuple[dict, dict, int]:
    """
    For pre-defined indicators, check if msci information fulfill threshold.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with indicator, operator and threshold information
    msci_information: dict
        msci data

    Returns
    dict
        Dictionary with binary value for each indicator
    dict
        Dictionary with na values from indicators
    int
        number of non-cleared indicators
    """
    operators = {">": operator.gt, "<": operator.lt, "=": operator.eq}
    flag_d = dict()
    na_d = dict()
    counter = 0
    # count flags
    for index, row in df.iterrows():
        i = row["Indicator Field Name"]
        v = msci_information[i]
        o = row["Operator"]
        ft = row["Flag_Threshold"]
        if pd.isna(v):
            flag_d[i + "_Flag"] = 1
            na_d[i] = 1
            counter += 1
        elif operators[o](v, ft):
            flag_d[i + "_Flag"] = 1
            counter += 1
        else:
            flag_d[i + "_Flag"] = 0
    return flag_d, na_d, counter


def exclude_rule(value: str, exclusions: list, **kwargs) -> bool:
    """
    Checks if a value is in exclusion list
    (list of industries company's industry should not be in to be included in theme)
    if industry is excluded, return False, else True

    Parameters
    ----------
    value: str
        name of industry company belongs to
    exclusions: list
        list of industries for exclusion

    Returns
    -------
    bool
        industry excluded
    """
    if value in exclusions:
        return False
    return True


def include_rule(value: str, inclusions: list, **kwargs) -> bool:
    """
    Checks if a value is in inclusion list
    (list of industries company's industry should be in to be included in theme)

    Parameters
    ----------
    value: str
        name of industry company belongs to
    exclusions: list
        list of industries for inclusion

    Returns
    -------
    bool
        industry included
    """
    if value in inclusions:
        return True
    return False


def bool_rule(b: bool, **kwargs) -> bool:
    """
    Check if boolean is True.

    Parameters
    ----------
    b: bool
        boolean to be checked

    Returns
    -------
    bool
        input is True
    """
    if b == True:
        return True
    return False


def reverse_bool_rule(b: bool, **kwargs) -> bool:
    """
    Reverse a boolean: if True, return False. If False or nan, return True

    Parameters
    ----------
    b: bool
        boolean to be checked

    Returns
    -------
    bool
        Input is False
    """
    if b == True:
        return False
    return True


def eq_rule(val: float, threshold: float, **kwargs) -> bool:
    """
    Check if value inputted is equal to specified threshold

    Parameters
    ----------
    val: float
        value
    threshold: float
        threshold value

    Returns
    -------
    bool
        input is bigger than threshold
    """
    if val == threshold:
        return True
    return False


def bigger_eq_rule(val: float, threshold: float, **kwargs) -> bool:
    """
    Check if value inputted is bigger or equal than specified threshold

    Parameters
    ----------
    val: float
        value
    threshold: float
        threshold value

    Returns
    -------
    bool
        input is bigger than threshold
    """
    if val >= threshold:
        return True
    return False


def bigger_rule(val: float, threshold: float, **kwargs) -> bool:
    """
    Check if value inputted is bigger than specified threshold

    Parameters
    ----------
    val: float
        value
    threshold: float
        threshold value

    Returns
    -------
    bool
        input is bigger than threshold
    """
    if val > threshold:
        return True
    return False
