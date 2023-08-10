import json
import os
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

    return configs
