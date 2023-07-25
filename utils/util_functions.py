import pandas as pd
from typing import Union


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
    for iss in input_list:
        if not (pd.isna(iss) or iss in output_list):
            output_list.append(iss)
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
            replace_dictionary(v, original_d[k])
    return original_d


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
