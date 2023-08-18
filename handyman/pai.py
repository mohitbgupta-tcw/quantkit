import quantkit.runner_PAI as runner
import quantkit.utils.configs as configs
import quantkit.handyman.msci_data_loader as msci_data_loaders
import quantkit.utils.util_functions as util_functions
import pandas as pd
import numpy as np
import json
import os


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

    name_mapping = {
        "CARBON_EMISSIONS_SCOPE_1": "1.1 - Scope 1 GHG Emissions",
        "CARBON_EMISSIONS_SCOPE_2": "1.2 - Scope 2 GHG Emissions",
        "CARBON_EMISSIONS_SCOPE_3_TOTAL": "1.3 - Scope 3 GHG Emissions",
        "CARBON_EMISSIONS_SCOPE123": "1.4 - Total GHG Emissions (Scope 1,2,3)",
        "Carbon_Footprint": "2 - Carbon Footprint (Scope 1+2+3)",
        "WACI": "3 - Weighted Average Carbon Intensity (Scope 1+2+3)",
        "Fossil_Fuel_Exposure": "4 - Fossil Fuel Exposure",
        "PCT_NONRENEW_CONSUMP_PROD_WEIGHTED": "5 - Non-Renewable Energy Consumption and Production",
        "Energy_Consumption_A": "6A - Energy Consumption Intensity by NACE (Agriculture, Foresty, Fishing)",
        "Energy_Consumption_B": "6B - Energy Consumption Intensity by NACE (Mining and Quarrying)",
        "Energy_Consumption_C": "6C - Energy Consumption Intensity by NACE (Manufacturing)",
        "Energy_Consumption_D": "6D - Energy Consumption Intensity by NACE (Electricity, Gas Steam and Air Conditioning Supply)",
        "Energy_Consumption_E": "6E - Energy Consumption Intensity by NACE (Manufacturing)",
        "Energy_Consumption_F": "6F - Energy Consumption Intensity by NACE (Contruction)",
        "Energy_Consumption_G": "6G - Energy Consumption Intensity by NACE (Wholesale and Retail Trade)",
        "Energy_Consumption_H": "6H - Energy Consumption Intensity by NACE (Water Transport)",
        "Energy_Consumption_L": "6L - Energy Consumption Intensity by NACE (Real Estate Activities)",
        "Biodiversity_Controv": "7 - Activities Neg Affecting Biodiversity",
        "WATER_EM": "8 - Emissions to Water",
        "HAZARD_WASTE": "9 - Hazardous Waste Ratio",
        "UN_violations": "10 - Violations of UNGC and OECD",
        "MECH_UN_GLOBAL_COMPACT": "11 - Lack of Processes to Monitor of UNGC and OECD",
        "GENDER_PAY_GAP_RATIO": "12 - Unadjusted Gender Pay Gap",
    }

    data = []
    for p in r.portfolio_datasource.portfolios:
        p_object = r.portfolio_datasource.portfolios[p]
        as_of_date = p_object.as_of_date
        for i in p_object.impact_data:
            impact = p_object.impact_data[i]["impact"]
            cov = p_object.impact_data[i]["coverage"]
            name = name_mapping[i]
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
