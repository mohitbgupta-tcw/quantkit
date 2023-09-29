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
    for p, port_store in r.portfolio_datasource.portfolios.items():
        as_of_date = port_store.as_of_date
        for i in port_store.impact_data:
            impact = {
                "Portfolio": p,
                "As of Date": as_of_date,
                "Principal Adverse Impact": mapping_configs.pai_mapping[i],
                "Coverage": port_store.impact_data[i]["coverage"],
                "Value": port_store.impact_data[i]["impact"],
            }
            data.append(impact)
    df = pd.DataFrame(data)
    return df
