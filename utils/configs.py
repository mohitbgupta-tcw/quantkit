import json
import os
import datetime
from pandas.tseries.offsets import BDay
import quantkit.utils.util_functions as util_functions
from typing import Union


def read_configs(local_configs: Union[str, dict] = "") -> dict:
    """
    read configs file from quantkit\\utils\\configs.json
    if there are local changes, overwrite existing configs file

    Parameters
    ----------
    local_configs: str | dict, optional
        path to a local configarations file or local configs as dict

    Returns
    -------
    dict
        configs file as dictionary
    """
    with open("quantkit/utils/configs.json") as f_in:
        configs = json.load(f_in)

    # local configs file exists
    if isinstance(local_configs, dict):
        configs = util_functions.replace_dictionary(local_configs, configs)
    elif os.path.isfile(local_configs):
        with open(local_configs) as f_in:
            configs_local = json.load(f_in)

        configs = util_functions.replace_dictionary(configs_local, configs)

    if not "as_of_date" in configs:
        today = datetime.datetime.today()
        last_bday = today - BDay(1)
        as_of_date = last_bday.strftime("%m/%d/%Y")
        configs["as_of_date"] = as_of_date

    return configs
