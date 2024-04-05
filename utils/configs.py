import json
import os
import datetime
from pandas.tseries.offsets import BDay
import quantkit.utils.util_functions as util_functions
from typing import Union


def read_configs(local_configs: Union[str, dict] = "", runner_type: str = None) -> dict:
    """
    read configs file from quantkit\\utils\\configs.json
    if there are local changes, overwrite existing configs file

    Parameters
    ----------
    local_configs: str | dict, optional
        path to a local configarations file or local configs as dict
    runner_type: str, optional
        runner type to include configs for, p.e. "risk_framework", "backtester", "pai"

    Returns
    -------
    dict
        configs file as dictionary
    """
    with open("quantkit/configs/configs.json") as f_in:
        configs = json.load(f_in)

    if runner_type:
        with open(f"quantkit/configs/{runner_type}.json") as runner_in:
            configs_runner = json.load(runner_in)

        configs = util_functions.replace_dictionary(configs_runner, configs)

    # local configs file exists
    if isinstance(local_configs, dict):
        configs = util_functions.replace_dictionary(local_configs, configs)
    elif os.path.isfile(local_configs):
        with open(local_configs) as f_in:
            configs_local = json.load(f_in)

        configs = util_functions.replace_dictionary(configs_local, configs)

    today = datetime.datetime.today()
    last_bday = today - BDay(1)
    as_of_date = last_bday.strftime("%m/%d/%Y")
    if "as_of_date" in configs:
        configs["portfolio_datasource"]["start_date"] = configs["as_of_date"]
        configs["portfolio_datasource"]["end_date"] = configs["as_of_date"]
    if "start_date" not in configs["portfolio_datasource"]:
        configs["portfolio_datasource"]["start_date"] = as_of_date
    if "end_date" not in configs["portfolio_datasource"]:
        configs["portfolio_datasource"]["end_date"] = as_of_date

    return configs
