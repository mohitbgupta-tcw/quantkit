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


def principal_adverse_data_points(local_configs: str = "") -> pd.DataFrame:
    """
    Run Principal Adverse Impact calculation
    and return detailed DataFrame on security level

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
        for s, holdings_d in port_store.holdings.items():
            sec_store = holdings_d["object"]
            comp_store = sec_store.parent_store
            holding_measures = holdings_d["holding_measures"]
            for h in holding_measures:
                sec_data = {
                    "As Of Date": port_store.as_of_date,
                    "Portfolio ISIN": port_store.id,
                    "Portfolio Name": port_store.name,
                    "Portfolio Weight": h["Portfolio_Weight"],
                    "Security ISIN": s,
                    "Issuer ISIN": comp_store.isin,
                    "MSCI ISSUERID": comp_store.msci_information["ISSUERID"],
                    "Security Name": sec_store.information["Security_Name"],
                    "Issuer Name": comp_store.msci_information["ISSUER_NAME"],
                    "Ticker": comp_store.msci_information["ISSUER_TICKER"],
                    "Labeled ESG Type": sec_store.information["Labeled ESG Type"],
                    "Sector Level 2": sec_store.information["Sector Level 2"],
                    "ISSUER_CNTRY_DOMICILE": comp_store.msci_information[
                        "ISSUER_CNTRY_DOMICILE"
                    ],
                    "GICS_SUB_IND": comp_store.msci_information["GICS_SUB_IND"],
                    "CARBON_EMISSIONS_SCOPE_1": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE_1"
                    ],
                    "CARBON_EMISSIONS_SCOPE_1_KEY": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE_1_KEY"
                    ],
                    "CARBON_EMISSIONS_SCOPE_2": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE_2"
                    ],
                    "CARBON_EMISSIONS_SCOPE_1_KEY": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE_1_KEY"
                    ],
                    "CARBON_EMISSIONS_SCOPE_3_TOTAL": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE_3_TOTAL"
                    ],
                    "CARBON_EMISSIONS_SCOPE_3_ESTIMATES_YEAR": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE_3_ESTIMATES_YEAR"
                    ],
                    "CARBON_EMISSIONS_SCOPE123": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE123"
                    ],
                    "CARBON_EMISSIONS_SOURCE": comp_store.msci_information[
                        "CARBON_EMISSIONS_SOURCE"
                    ],
                    "CARBON_EMISSIONS_YEAR": comp_store.msci_information[
                        "CARBON_EMISSIONS_YEAR"
                    ],
                    "EVIC_EUR": comp_store.msci_information["EVIC_EUR"],
                    "CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN": comp_store.msci_information[
                        "CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN"
                    ],
                    "ACTIVE_FF_SECTOR_EXPOSURE": comp_store.msci_information[
                        "ACTIVE_FF_SECTOR_EXPOSURE"
                    ],
                    "ACTIVE_FF_SECTOR_EXPOSURE_SOURCE": comp_store.msci_information[
                        "ACTIVE_FF_SECTOR_EXPOSURE_SOURCE"
                    ],
                    "ACTIVE_FF_SECTOR_EXPOSURE_YEAR": comp_store.msci_information[
                        "ACTIVE_FF_SECTOR_EXPOSURE_YEAR"
                    ],
                    "PCT_NONRENEW_CONSUMP_PROD": comp_store.msci_information[
                        "PCT_NONRENEW_CONSUMP_PROD"
                    ],
                    "NACE_SECTION_CODE": comp_store.msci_information[
                        "NACE_SECTION_CODE"
                    ],
                    "NACE_SECTION_DESCRIPTION": comp_store.msci_information[
                        "NACE_SECTION_DESCRIPTION"
                    ],
                    "ENERGY_CONSUMP_INTEN_EUR": comp_store.msci_information[
                        "ENERGY_CONSUMP_INTEN_EUR"
                    ],
                    "ENERGY_CONSUMP_INTEN_EUR_YEAR": comp_store.msci_information[
                        "ENERGY_CONSUMP_INTEN_EUR_YEAR"
                    ],
                    "OPS_PROT_BIODIV_CONTROVS": comp_store.msci_information[
                        "OPS_PROT_BIODIV_CONTROVS"
                    ],
                    "WATER_EM_EFF_METRIC_TONS": comp_store.msci_information[
                        "WATER_EM_EFF_METRIC_TONS"
                    ],
                    "WATER_EM_EFF_METRIC_TONS_YEAR": comp_store.msci_information[
                        "WATER_EM_EFF_METRIC_TONS_YEAR"
                    ],
                    "HAZARD_WASTE_METRIC_TON": comp_store.msci_information[
                        "HAZARD_WASTE_METRIC_TON"
                    ],
                    "HAZARD_WASTE_METRIC_TON_YEAR": comp_store.msci_information[
                        "HAZARD_WASTE_METRIC_TON_YEAR"
                    ],
                    "OVERALL_FLAG": comp_store.msci_information["OVERALL_FLAG"],
                    "MECH_UN_GLOBAL_COMPACT": comp_store.msci_information[
                        "MECH_UN_GLOBAL_COMPACT"
                    ],
                    "GENDER_PAY_GAP_RATIO": comp_store.msci_information[
                        "GENDER_PAY_GAP_RATIO"
                    ],
                    "GENDER_PAY_GAP_RATIO_SOURCE": comp_store.msci_information[
                        "GENDER_PAY_GAP_RATIO_SOURCE"
                    ],
                    "GENDER_PAY_GAP_YEAR": comp_store.msci_information[
                        "GENDER_PAY_GAP_YEAR"
                    ],
                    "FEMALE_DIRECTORS_PCT": comp_store.msci_information[
                        "FEMALE_DIRECTORS_PCT"
                    ],
                    "CONTRO_WEAP_CBLMBW_ANYTIE": comp_store.msci_information[
                        "CONTRO_WEAP_CBLMBW_ANYTIE"
                    ],
                    "CONTRO_WEAP_CBLMBW_ANYTIE_SOURCE": comp_store.msci_information[
                        "CONTRO_WEAP_CBLMBW_ANYTIE_SOURCE"
                    ],
                    "CTRY_GHG_INTEN_GDP_EUR": comp_store.msci_information[
                        "CTRY_GHG_INTEN_GDP_EUR"
                    ],
                    "GOVERNMENT_EU_SANCTIONS": comp_store.msci_information[
                        "GOVERNMENT_EU_SANCTIONS"
                    ],
                    "GOVERNMENT_UN_SANCTIONS": comp_store.msci_information[
                        "GOVERNMENT_UN_SANCTIONS"
                    ],
                    "CARBON_EMISSIONS_REDUCT_INITIATIVES": comp_store.msci_information[
                        "CARBON_EMISSIONS_REDUCT_INITIATIVES"
                    ],
                    "CARBON_EMISSIONS_REDUCT_INITIATIVES_SOURCE": comp_store.msci_information[
                        "CARBON_EMISSIONS_REDUCT_INITIATIVES_SOURCE"
                    ],
                    "WORKPLACE_ACC_PREV_POL": comp_store.msci_information[
                        "WORKPLACE_ACC_PREV_POL"
                    ],
                    "WORKPLACE_ACC_PREV_POL_SOURCE": comp_store.msci_information[
                        "WORKPLACE_ACC_PREV_POL_SOURCE"
                    ],
                    "TOTL_ENRGY_CONSUMP_YEAR": comp_store.msci_information[
                        "TOTL_ENRGY_CONSUMP_YEAR"
                    ],
                    "TOTL_ERGY_CONSUMP_GWH_SOURCE": comp_store.msci_information[
                        "TOTL_ERGY_CONSUMP_GWH_SOURCE"
                    ],
                    "TOTL_ERGY_CONSUMP_NONRENEW_GWH_SOURCE": comp_store.msci_information[
                        "TOTL_ERGY_CONSUMP_NONRENEW_GWH_SOURCE"
                    ],
                    "TOTL_ERGY_CONSUMP_NONRENEW_GWH_YEAR": comp_store.msci_information[
                        "TOTL_ERGY_CONSUMP_NONRENEW_GWH_YEAR"
                    ],
                    "TOTL_ERGY_CONSUMP_PRODUCT_NONRENEW_GWH_YEAR": comp_store.msci_information[
                        "TOTL_ERGY_CONSUMP_PRODUCT_NONRENEW_GWH_YEAR"
                    ],
                }
                data.append(sec_data)
    df = pd.DataFrame(data)
    return df
