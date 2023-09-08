import quantkit.runners.runner_PAI as runner
import quantkit.utils.mapping_configs as mapping_configs
import pandas as pd
import numpy as np


def principal_adverse_impact(local_configs: str = "") -> pd.DataFrame:
    """
    Run Principal Adverse Impact calculation
    and return detailed DataFrame with portfolio information

    Parameters
    ----------
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        Detailed DataFrame
    """
    r = runner.Runner()
    r.init(local_configs=local_configs)
    r.run()

    data = []
    for p in r.portfolio_datasource.portfolios:
        p_object = r.portfolio_datasource.portfolios[p]
        as_of_date = p_object.as_of_date
        for i in p_object.impact_data:
            impact = p_object.impact_data[i]["impact"]
            cov = p_object.impact_data[i]["coverage"]
            name = mapping_configs.pai_mapping[i]
            data.append((p, as_of_date, name, impact, cov))
    df = pd.DataFrame(
        data,
        columns=[
            "Portfolio",
            "As of Date",
            "Principal Adverse Impact",
            "Value",
            "Coverage",
        ],
    )
    return df
