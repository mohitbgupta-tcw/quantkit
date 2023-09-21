import quantkit.runners.runner_risk_framework as runner_risk_framework
import quantkit.utils.configs as configs
import quantkit.handyman.msci_data_loader as msci_data_loaders
import quantkit.data_sources.snowflake as snowflake
import quantkit.visualization.risk_framework.esg_characteristics as esg_characteristics
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.utils.snowflake_utils as snowflake_utils
import pandas as pd
import numpy as np
import json
import os


def risk_framework(local_configs: str = "") -> pd.DataFrame:
    """
    Run risk framework and return detailed DataFrame with portfolio and
    security information

    Parameters
    ----------
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        Detailed DataFrame
    """
    r = runner_risk_framework.Runner()
    r.init(local_configs=local_configs)
    r.run()
    data_detail = []
    for p, port_store in r.portfolio_datasource.portfolios.items():
        for s, holdings_d in port_store.holdings.items():
            sec_store = holdings_d["object"]
            comp_store = sec_store.parent_store
            holding_measures = holdings_d["holding_measures"]
            for h in holding_measures:
                portfolio_weight = h["Portfolio_Weight"]
                oas = h["OAS"]
                sec_data = {
                    "As Of Date": port_store.as_of_date,
                    "Portfolio ISIN": port_store.id,
                    "Portfolio Name": port_store.name,
                    "Security ISIN": s,
                    "Issuer ISIN": comp_store.isin,
                    "Security Name": sec_store.information["Security_Name"],
                    "Issuer Name": comp_store.msci_information["ISSUER_NAME"],
                    "Ticker": comp_store.msci_information["ISSUER_TICKER"],
                    "CUSIP": comp_store.msci_information["ISSUER_CUSIP"],
                    "Country of Risk": sec_store.information["Country_of_Risk"],
                    "Analyst": comp_store.information["Sub-Industry"].information[
                        "Analyst"
                    ],
                    "Issuer Country": comp_store.information[
                        "Issuer_Country"
                    ].information["Country"],
                    "Portfolio Weight": portfolio_weight,
                    "OAS": oas,
                    "CARBON_EMISSIONS_SCOPE_12_INTEN": comp_store.msci_information[
                        "CARBON_EMISSIONS_SCOPE_12_INTEN"
                    ],
                    "Labeled ESG Type": sec_store.information["Labeled_ESG_Type"],
                    "Sector Level 2": comp_store.information["Sector_Level_2"],
                    "ESG Collateral Type": sec_store.information["ESG_Collateral_Type"][
                        "ESG Collat Type"
                    ],
                    "JPM Sector": sec_store.information["JPM_Sector"],
                    "Industry": comp_store.information["Industry"].name,
                    "BCLASS_Level2": sec_store.information["BCLASS_Level2"],
                    "BCLASS_Level3": sec_store.information["BCLASS_Level3"],
                    "BCLASS": comp_store.information["BCLASS_Level4"].class_name,
                    "BCLASS_SECTOR": comp_store.information[
                        "BCLASS_Level4"
                    ].information["INDUSTRY_BCLASS_LEVEL3"],
                    "GICS": comp_store.information["GICS_SUB_IND"].class_name,
                    "GICS_SECTOR": comp_store.information["GICS_SUB_IND"].information[
                        "SECTOR"
                    ],
                    "SCLASS_Level1": sec_store.information["SClass_Level1"],
                    "SCLASS_Level2": sec_store.information["SClass_Level2"],
                    "SCLASS_Level3": sec_store.information["SClass_Level3"],
                    "SCLASS_Level4": sec_store.information["SClass_Level4"],
                    "SCLASS_Level4-P": sec_store.information["SClass_Level4-P"],
                    "SCLASS-Level5": sec_store.information["SClass_Level5"],
                    "Alcohol": comp_store.information["Exclusion_d"].get("Alcohol", 0),
                    "Tobacco": comp_store.information["Exclusion_d"].get("Tobacco", 0),
                    "Oil_Gas": comp_store.information["Exclusion_d"].get("Oil_Gas", 0),
                    "Gambling": comp_store.information["Exclusion_d"].get(
                        "Gambling", 0
                    ),
                    "Weapons_Firearms": comp_store.information["Exclusion_d"].get(
                        "Weapons_Firearms", 0
                    ),
                    "Thermal_Coal_Mining": comp_store.information["Exclusion_d"].get(
                        "Thermal_Coal_Mining", 0
                    ),
                    "Thermal_Coal_Power_Gen": comp_store.information["Exclusion_d"].get(
                        "Thermal_Coal_Power_Gen", 0
                    ),
                    "Controversial_Weapons": comp_store.information["Exclusion_d"].get(
                        "Controversial_Weapons", 0
                    ),
                    "UN_Alignement": comp_store.information["Exclusion_d"].get(
                        "UN_Alignement", 0
                    ),
                    "Adult_Entertainment": comp_store.information["Exclusion_d"].get(
                        "Adult_Entertainment", 0
                    ),
                    "Review Flag": comp_store.scores["Review_Flag"],
                    "Review Comments": comp_store.scores["Review_Comments"],
                    "INDUSTRY_ADJUSTED_SCORE": comp_store.msci_information[
                        "INDUSTRY_ADJUSTED_SCORE"
                    ],
                    "GOVERNMENT_ADJUSTED_ESG_SCORE": comp_store.msci_information[
                        "GOVERNMENT_ADJUSTED_ESG_SCORE"
                    ],
                    "GOVERNMENT_ESG_RATING": comp_store.msci_information[
                        "GOVERNMENT_ESG_RATING"
                    ],
                    "Muni Score": comp_store.scores["Muni_Score"],
                    "Securitized Score": sec_store.scores["Securitized_Score"],
                    "Sovereign Score": comp_store.scores["Sovereign_Score"],
                    "ESRM Score": comp_store.scores["ESRM_Score"],
                    "ESRM_flagged": sum(comp_store.scores["ESRM_Flags"].values()),
                    "NA_Flags_ESRM": sum(comp_store.scores["NA_Flags_ESRM"].values()),
                    "PRIVACY_DATA_SEC_EXP_SCORE_Flag": comp_store.scores[
                        "ESRM_Flags"
                    ].get("PRIVACY_DATA_SEC_EXP_SCORE_Flag", 0),
                    "PRIVACY_DATA_SEC_EXP_SCORE": comp_store.msci_information[
                        "PRIVACY_DATA_SEC_EXP_SCORE"
                    ],
                    "COMM_REL_RISK_EXP_SCORE_Flag": comp_store.scores["ESRM_Flags"].get(
                        "COMM_REL_RISK_EXP_SCORE_Flag", 0
                    ),
                    "COMM_REL_RISK_EXP_SCORE": comp_store.msci_information[
                        "COMM_REL_RISK_EXP_SCORE"
                    ],
                    "LABOR_MGMT_EXP_SCORE_Flag": comp_store.scores["ESRM_Flags"].get(
                        "LABOR_MGMT_EXP_SCORE_Flag", 0
                    ),
                    "LABOR_MGMT_EXP_SCORE": comp_store.msci_information[
                        "LABOR_MGMT_EXP_SCORE"
                    ],
                    "WATER_STRESS_EXP_SCORE_Flag": comp_store.scores["ESRM_Flags"].get(
                        "WATER_STRESS_EXP_SCORE_Flag", 0
                    ),
                    "WATER_STRESS_EXP_SCORE": comp_store.msci_information[
                        "WATER_STRESS_EXP_SCORE"
                    ],
                    "ENERGY_EFFICIENCY_EXP_SCORE_Flag": comp_store.scores[
                        "ESRM_Flags"
                    ].get("ENERGY_EFFICIENCY_EXP_SCORE_Flag", 0),
                    "ENERGY_EFFICIENCY_EXP_SCORE": comp_store.msci_information[
                        "ENERGY_EFFICIENCY_EXP_SCORE"
                    ],
                    "BIODIV_LAND_USE_EXP_SCORE_Flag": comp_store.scores[
                        "ESRM_Flags"
                    ].get("BIODIV_LAND_USE_EXP_SCORE_Flag", 0),
                    "BIODIV_LAND_USE_EXP_SCORE": comp_store.msci_information[
                        "BIODIV_LAND_USE_EXP_SCORE"
                    ],
                    "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL_Flag": comp_store.scores[
                        "ESRM_Flags"
                    ].get("CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL_Flag", 0),
                    "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL": comp_store.msci_information[
                        "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL"
                    ],
                    "CUSTOMER_RELATIONS_SCORE_Flag": comp_store.scores[
                        "ESRM_Flags"
                    ].get("CUSTOMER_RELATIONS_SCORE_Flag", 0),
                    "CUSTOMER_RELATIONS_SCORE": comp_store.msci_information[
                        "CUSTOMER_RELATIONS_SCORE"
                    ],
                    "PROD_SFTY_QUALITY_EXP_SCORE_Flag": comp_store.scores[
                        "ESRM_Flags"
                    ].get("PROD_SFTY_QUALITY_EXP_SCORE_Flag", 0),
                    "PROD_SFTY_QUALITY_EXP_SCORE": comp_store.msci_information[
                        "PROD_SFTY_QUALITY_EXP_SCORE"
                    ],
                    "IVA_COMPANY_RATING": comp_store.msci_information[
                        "IVA_COMPANY_RATING"
                    ],
                    "OVERALL_FLAG": comp_store.msci_information["OVERALL_FLAG"],
                    "UNGC_COMPLIANCE": comp_store.msci_information["UNGC_COMPLIANCE"],
                    "Governance Score": comp_store.scores["Governance_Score"],
                    "Governance_flagged": sum(
                        comp_store.scores["Governance_Flags"].values()
                    ),
                    "NA_Flags_Governance": sum(
                        comp_store.scores["NA_Flags_Governance"].values()
                    ),
                    "CARBON_EMISSIONS_CDP_DISCLOSURE_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("CARBON_EMISSIONS_CDP_DISCLOSURE_Flag", 0),
                    "CARBON_EMISSIONS_CDP_DISCLOSURE": comp_store.msci_information[
                        "CARBON_EMISSIONS_CDP_DISCLOSURE"
                    ],
                    "COMBINED_CEO_CHAIR_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("COMBINED_CEO_CHAIR_Flag", 0),
                    "COMBINED_CEO_CHAIR": comp_store.msci_information[
                        "COMBINED_CEO_CHAIR"
                    ],
                    "CONTROLLING_SHAREHOLDER_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("CONTROLLING_SHAREHOLDER_Flag", 0),
                    "CONTROLLING_SHAREHOLDER": comp_store.msci_information[
                        "CONTROLLING_SHAREHOLDER"
                    ],
                    "CROSS_SHAREHOLDINGS_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("CROSS_SHAREHOLDINGS_Flag", 0),
                    "CROSS_SHAREHOLDINGS": comp_store.msci_information[
                        "CROSS_SHAREHOLDINGS"
                    ],
                    "FEMALE_DIRECTORS_PCT_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("FEMALE_DIRECTORS_PCT_Flag", 0),
                    "FEMALE_DIRECTORS_PCT": comp_store.msci_information[
                        "FEMALE_DIRECTORS_PCT"
                    ],
                    "INDEPENDENT_BOARD_MAJORITY_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("INDEPENDENT_BOARD_MAJORITY_Flag", 0),
                    "INDEPENDENT_BOARD_MAJORITY": comp_store.msci_information[
                        "INDEPENDENT_BOARD_MAJORITY"
                    ],
                    "NEGATIVE_DIRECTOR_VOTES_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("NEGATIVE_DIRECTOR_VOTES_Flag", 0),
                    "NEGATIVE_DIRECTOR_VOTES": comp_store.msci_information[
                        "NEGATIVE_DIRECTOR_VOTES"
                    ],
                    "PAY_LINKED_TO_SUSTAINABILITY_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("PAY_LINKED_TO_SUSTAINABILITY_Flag", 0),
                    "PAY_LINKED_TO_SUSTAINABILITY": comp_store.msci_information[
                        "PAY_LINKED_TO_SUSTAINABILITY"
                    ],
                    "POISON_PILL_Flag": comp_store.scores["Governance_Flags"].get(
                        "POISON_PILL_Flag", 0
                    ),
                    "POISON_PILL": comp_store.msci_information["POISON_PILL"],
                    "WOMEN_EXEC_MGMT_PCT_RECENT_Flag": comp_store.scores[
                        "Governance_Flags"
                    ].get("WOMEN_EXEC_MGMT_PCT_RECENT_Flag", 0),
                    "WOMEN_EXEC_MGMT_PCT_RECENT": comp_store.msci_information[
                        "WOMEN_EXEC_MGMT_PCT_RECENT"
                    ],
                    "Transition Score": comp_store.scores["Transition_Score"],
                    "Carbon Intensity (Scope 123)": comp_store.information.get(
                        "Carbon Intensity (Scope 123)", 0
                    ),
                    "ClimateGHGReductionTargets": comp_store.sdg_information[
                        "ClimateGHGReductionTargets"
                    ],
                    "HAS_SBTI_APPROVED_TARGET": comp_store.msci_information[
                        "HAS_SBTI_APPROVED_TARGET"
                    ],
                    "HAS_COMMITTED_TO_SBTI_TARGET": comp_store.msci_information[
                        "HAS_COMMITTED_TO_SBTI_TARGET"
                    ],
                    "CapEx": comp_store.information.get("CapEx", 0),
                    "Climate_Revenue": comp_store.information.get("Climate_Revenue", 0),
                    "RENEWENERGY_MSCI": comp_store.information.get(
                        "RENEWENERGY_MSCI", 0
                    ),
                    "RENEWENERGY_ISS": comp_store.information.get("RENEWENERGY_ISS", 0),
                    "MOBILITY_MSCI": comp_store.information.get("MOBILITY_MSCI", 0),
                    "MOBILITY_ISS": comp_store.information.get("MOBILITY_ISS", 0),
                    "CIRCULARITY_MSCI": comp_store.information.get(
                        "CIRCULARITY_MSCI", 0
                    ),
                    "CIRCULARITY_ISS": comp_store.information.get("CIRCULARITY_ISS", 0),
                    "CCADAPT_MSCI": comp_store.information.get("CCADAPT_MSCI", 0),
                    "CCADAPT_ISS": comp_store.information.get("CCADAPT_ISS", 0),
                    "BIODIVERSITY_MSCI": comp_store.information.get(
                        "BIODIVERSITY_MSCI", 0
                    ),
                    "BIODIVERSITY_ISS": comp_store.information.get(
                        "BIODIVERSITY_ISS", 0
                    ),
                    "SMARTCITIES_MSCI": comp_store.information.get(
                        "SMARTCITIES_MSCI", 0
                    ),
                    "SMARTCITIES_ISS": comp_store.information.get("SMARTCITIES_ISS", 0),
                    "EDU_MSCI": comp_store.information.get("EDU_MSCI", 0),
                    "EDU_ISS": comp_store.information.get("EDU_ISS", 0),
                    "HEALTH_MSCI": comp_store.information.get("HEALTH_MSCI", 0),
                    "HEALTH_ISS": comp_store.information.get("HEALTH_ISS", 0),
                    "SANITATION_MSCI": comp_store.information.get("SANITATION_MSCI", 0),
                    "SANITATION_ISS": comp_store.information.get("SANITATION_ISS", 0),
                    "INCLUSION_MSCI": comp_store.information.get("INCLUSION_MSCI", 0),
                    "INCLUSION_ISS": comp_store.information.get("INCLUSION_ISS", 0),
                    "NUTRITION_MSCI": comp_store.information.get("NUTRITION_MSCI", 0),
                    "NUTRITION_ISS": comp_store.information.get("NUTRITION_ISS", 0),
                    "AFFORDABLE_MSCI": comp_store.information.get("AFFORDABLE_MSCI", 0),
                    "AFFORDABLE_ISS": comp_store.information.get("AFFORDABLE_ISS", 0),
                    "Risk_Score_Overall": sec_store.scores["Risk_Score_Overall"],
                }

                if (
                    port_store.id in r.params["A8Funds"]
                    and "Article 8" in comp_store.information["Exclusion"]
                    and not (
                        sec_store.information["Labeled_ESG_Type"]
                        in [
                            "Labeled Green",
                            "Labeled Social",
                            "Labeled Sustainable",
                            "Labeled Sustainable Linked",
                        ]
                        and comp_store.information["BCLASS_Level4"].class_name
                        in r.params["carve_out_sectors"]
                    )
                ):
                    sec_data["SCLASS_Level1"] = "Excluded"
                    sec_data["SCLASS_Level2"] = "Exclusion"
                    sec_data["SCLASS_Level3"] = "Exclusion"
                    sec_data["SCLASS_Level4"] = "Excluded Sector"
                    sec_data["SCLASS_Level4-P"] = "Excluded Sector"
                elif (
                    port_store.id in r.params["A9Funds"]
                    and "Article 9" in comp_store.information["Exclusion"]
                    and not (
                        sec_store.information["Labeled_ESG_Type"]
                        in [
                            "Labeled Green",
                            "Labeled Social",
                            "Labeled Sustainable",
                            "Labeled Sustainable Linked",
                        ]
                        and comp_store.information["BCLASS_Level4"].class_name
                        in r.params["carve_out_sectors"]
                    )
                ):
                    sec_data["SCLASS_Level1"] = "Excluded"
                    sec_data["SCLASS_Level2"] = "Exclusion"
                    sec_data["SCLASS_Level3"] = "Exclusion"
                    sec_data["SCLASS_Level4"] = "Excluded Sector"
                    sec_data["SCLASS_Level4-P"] = "Excluded Sector"

                if "INTELSAT" in sec_store.information["Security_Name"]:
                    sec_data["SCLASS_Level1"] = "Preferred"
                    sec_data["SCLASS_Level2"] = "Sustainable Theme"
                    sec_data["SCLASS_Level3"] = "People"
                    sec_data["SCLASS_Level4"] = "INCLUSION"
                    sec_data["SCLASS_Level4-P"] = "INCLUSION"
                    sec_data["Governance Score"] = 4
                    sec_data["ESRM Score"] = 1
                    sec_data["Risk_Score_Overall"] = "Average ESG Score"

                data_detail.append(sec_data)

    df_detailed = pd.DataFrame(data_detail)
    return df_detailed


def sector_subset(
    gics_list: list = [], bclass_list: list = [], local_configs: str = ""
) -> pd.DataFrame:
    """
    Run risk framework and return scores for specific industries

    Parameters
    ----------
    gics_list: list, optional
        list of GICS_SUB_IND
    bclass_list: list, optional
        list of BCLASS_LEVEL4
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        Summary DataFrame for specific industries
    """
    df = risk_framework(local_configs=local_configs)
    df = df[df["BCLASS"].isin(bclass_list)]
    df = df[df["GICS"].isin(gics_list)]
    return df


def isin_lookup(isin_list: list, local_configs: str = "") -> pd.DataFrame:
    """
    For a list of ISINs, run the risk framework

    Parameters
    ----------
    isin_list: list
        list of isins
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        Summary DataFrame with score data for entered isins
    """

    # create portfolio sheet
    portfolio_df = pd.DataFrame()
    portfolio_df["ISIN"] = isin_list
    portfolio_df["As Of Date"] = pd.to_datetime("today").normalize()
    portfolio_df["Portfolio"] = "Test_Portfolio"
    portfolio_df["Portfolio Name"] = "Test_Portfolio"
    portfolio_df["ESG Collateral Type"] = "Unknown"
    portfolio_df["Issuer ESG"] = "No"
    portfolio_df["Loan Category"] = np.nan
    portfolio_df["Labeled ESG Type"] = "None"
    portfolio_df["ISSUER_NAME"] = isin_list
    portfolio_df["TCW ESG"] = "None"
    portfolio_df["Ticker Cd"] = np.nan
    portfolio_df["Sector Level 1"] = "Corporate"
    portfolio_df["Sector Level 2"] = "Industrial"
    portfolio_df["JPM Sector"] = np.nan
    portfolio_df["BCLASS_Level2"] = np.nan
    portfolio_df["BCLASS_Level3"] = np.nan
    portfolio_df["BCLASS_Level4"] = np.nan
    portfolio_df["Market Region"] = np.nan
    portfolio_df["Country of Risk"] = np.nan
    portfolio_df["Portfolio_Weight"] = 1 / len(isin_list)
    portfolio_df["Base Mkt Val"] = 1
    portfolio_df["Rating Raw MSCI"] = np.nan
    portfolio_df["OAS"] = np.nan
    portfolio_df = portfolio_df.to_json(orient="index")

    # create msci mapping file
    msci_df = msci_data_loaders.create_msci_mapping(
        isin_list=isin_list, local_configs=local_configs
    )
    msci_df = msci_df.to_json(orient="index")

    configs_overwrite = {
        "portfolio_datasource": {"source": 6, "json_str": portfolio_df},
        "security_datasource": {
            "msci": {"source": 6, "json_str": msci_df},
        },
    }

    if os.path.isfile(local_configs):
        with open(local_configs) as f_in:
            configs_local = json.load(f_in)
        c = {**configs_local, **configs_overwrite}
    else:
        configs_local = None
        c = configs_overwrite
    with open("quantkit\\params_temp.json", "w") as f:
        json.dump(c, f)

    # run framework
    df = risk_framework("quantkit\\params_temp.json")
    os.remove("quantkit\\params_temp.json")
    return df


def print_esg_characteristics_pdf(
    portfolio_isin: str,
    local_configs: str = "",
    filtered: bool = False,
    show_holdings: bool = False,
) -> None:
    """
    For a given portfolio, create ESG Sustainable Characteristics PDF template
    Please click link in cell output to open pdf

    Parameters
    ----------
    portfolio_isin: str
        portfolio to be shown
    local_configs: str, optional
        path to a local configarations file
    filtered: bool, optional
        filter portfolio holdings for corporates and quasi-sovereigns
    show_holdings: bool, optional
        show holdings of portfolio
    """
    df = snowflake_utils.load_from_snowflake(
        database="SANDBOX_ESG",
        schema="TIM_SCHEMA",
        table_name="Sustainability_Framework_Detailed",
        local_configs=local_configs,
    )

    benchmark = mapping_configs.portfolio_benchmark[portfolio_isin]
    all_portfolios = [portfolio_isin, benchmark]
    if portfolio_isin == "3750":
        all_portfolios.append("JPM EM Custom Index (50/50)")
    portfolio_type = mapping_configs.portfolio_type[portfolio_isin]
    df = df[df["Portfolio ISIN"].isin(all_portfolios)]
    pdf = esg_characteristics.ESGCharacteristics(
        title="Financial Report",
        data=df,
        portfolio_type=portfolio_type,
        portfolio=portfolio_isin,
        benchmark=benchmark,
        show_holdings=show_holdings,
        filtered=filtered,
    )
    pdf.run()
    pdf.app.run_server(debug=False, jupyter_mode="tab")
