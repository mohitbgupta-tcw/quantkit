import json
import os
import datetime
from pandas.tseries.offsets import BDay
import quantkit.utils.util_functions as util_functions


def read_configs(local_configs: str = "") -> dict:
    """
    read configs file from quantkit\\utils\\configs.json
    if there are local changes, overwrite existing configs file

    Parameters
    ----------
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    dict
        configs file as dictionary
    """
    with open("quantkit/utils/configs.json") as f_in:
        configs = json.load(f_in)

    # local configs file exists
    if os.path.isfile(local_configs):
        with open(local_configs) as f_in:
            configs_local = json.load(f_in)

        configs = util_functions.replace_dictionary(configs_local, configs)

    if not "as_of_date" in configs:
        today = datetime.datetime.today()
        last_bday = today - BDay(1)
        as_of_date = last_bday.strftime("%m/%d/%Y")
        configs["as_of_date"] = as_of_date

    return configs
