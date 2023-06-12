import json


def read_configs():
    """
    read configs file from quantkit\\utils\\configs.json

    Returns
    -------
    dict
        configs file as dictionary
    """
    with open("quantkit/utils/configs.json") as f_in:
        return json.load(f_in)
