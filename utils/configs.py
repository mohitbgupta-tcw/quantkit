import json


def read_configs() -> dict:
    """
    read configs file from quantkit\\utils\\configs.json
    if there are local changes, overwrite existing configs file

    Returns
    -------
    dict
        configs file as dictionary
    """
    with open("quantkit/utils/configs.json") as f_in:
        configs = json.load(f_in)

    local_configs_path = configs["configs_path"]
    with open(local_configs_path) as f_in:
        configs_local = json.load(f_in)

    for overwrite in configs_local:
        configs[overwrite] = configs_local[overwrite]
    return configs
