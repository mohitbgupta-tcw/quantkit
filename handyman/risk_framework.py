import quantkit.runner as runner
import quantkit.utils.configs as configs
import quantkit.utils.data_loaders as data_loaders
import pandas as pd
import numpy as np
import json


def risk_framework() -> pd.DataFrame:
    """
    Run risk framework and return detailed DataFrame with portfolio and
    security information

    Returns
    -------
    pd.DataFrame
        Detailed DataFrame
    """
    r = runner.Runner()
    r.init()
    r.run()
    data_detail = []
    for p in r.portfolio_datasource.portfolios:
        portfolio_isin = r.portfolio_datasource.portfolios[p].id
        portfolio_name = r.portfolio_datasource.portfolios[p].name
        for s in r.portfolio_datasource.portfolios[p].holdings:
            sec_store = r.portfolio_datasource.portfolios[p].holdings[s]["object"]
            comp_store = sec_store.parent_store
            issuer_name = sec_store.information["IssuerName"]
            iva_rating = comp_store.information["IVA_COMPANY_RATING"]
            r_flag = comp_store.scores["Review_Flag"]
            r_comments = comp_store.scores["Review_Comments"]
            s2 = comp_store.information["Sector_Level_2"]
            bclass = comp_store.information["BCLASS_Level4"].class_name
            gics = comp_store.information["GICS_SUB_IND"].class_name
            muni_score = comp_store.scores["Muni_Score"]
            sec_score = sec_store.scores["Securitized_Score"]
            sov_score = comp_store.scores["Sovereign_Score"]
            esrm_score = comp_store.scores["ESRM_Score"]
            gov_score = comp_store.scores["Governance_Score"]
            trans_score = comp_store.scores["Transition_Score"]
            level_1 = sec_store.information["SClass_Level1"]
            level_2 = sec_store.information["SClass_Level2"]
            level_3 = sec_store.information["SClass_Level3"]
            level_4 = sec_store.information["SClass_Level4"]
            level_4p = sec_store.information["SClass_Level4-P"]
            level_5 = sec_store.information["SClass_Level5"]
            risk_score_overall = sec_store.scores["Risk_Score_Overall"]
            labeled_esg_type = sec_store.information["Labeled_ESG_Type"]

            if (
                portfolio_isin in r.params["A8Funds"]
                and "Article 8" in comp_store.information["Exclusion"]
                and not (
                    labeled_esg_type
                    in [
                        "Labeled Green",
                        "Labeled Social",
                        "Labeled Sustainable",
                        "Labeled Sustainable Linked",
                    ]
                    and bclass in r.params["carve_out_sectors"]
                )
            ):
                level_1 = "Excluded"
                level_2 = "Exclusion"
                level_3 = "Exclusion"
                level_4 = "Excluded Sector"
                level_4p = "Excluded Sector"
            elif (
                portfolio_isin in r.params["A9Funds"]
                and "Article 9" in comp_store.information["Exclusion"]
                and not (
                    labeled_esg_type
                    in [
                        "Labeled Green",
                        "Labeled Social",
                        "Labeled Sustainable",
                        "Labeled Sustainable Linked",
                    ]
                    and bclass in r.params["carve_out_sectors"]
                )
            ):
                level_1 = "Excluded"
                level_2 = "Exclusion"
                level_3 = "Exclusion"
                level_4 = "Excluded Sector"
                level_4p = "Excluded Sector"

            if "INTELSAT" in sec_store.information["Security_Name"]:
                level_1 = "Preferred"
                level_2 = "Sustainable Theme"
                level_3 = "People"
                level_4 = "INCLUSION"
                level_4p = "INCLUSION"
                gov_score = 4
                esrm_score = 1

            holding_measures = r.portfolio_datasource.portfolios[p].holdings[s][
                "holding_measures"
            ]
            for h in holding_measures:
                portfolio_weight = h["Portfolio_Weight"]
                oas = h["OAS"]

                data_detail.append(
                    (
                        portfolio_isin,
                        portfolio_name,
                        s,
                        issuer_name,
                        comp_store.information["Issuer_Country"].information["Country"],
                        portfolio_weight,
                        oas,
                        comp_store.msci_information["CARBON_EMISSIONS_SCOPE_12_INTEN"],
                        labeled_esg_type,
                        s2,
                        bclass,
                        comp_store.information["BCLASS_Level4"].information[
                            "INDUSTRY_BCLASS_LEVEL3"
                        ],
                        gics,
                        comp_store.information["GICS_SUB_IND"].information["SECTOR"],
                        level_1,
                        level_2,
                        level_3,
                        level_4,
                        level_4p,
                        level_5,
                        r_flag,
                        r_comments,
                        muni_score,
                        sec_score,
                        sov_score,
                        esrm_score,
                        sum(comp_store.scores["ESRM_Flags"].values()),
                        sum(comp_store.scores["NA_Flags_ESRM"].values()),
                        comp_store.scores["ESRM_Flags"].get(
                            "PRIVACY_DATA_SEC_EXP_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["PRIVACY_DATA_SEC_EXP_SCORE"],
                        comp_store.scores["ESRM_Flags"].get(
                            "COMM_REL_RISK_EXP_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["COMM_REL_RISK_EXP_SCORE"],
                        comp_store.scores["ESRM_Flags"].get(
                            "LABOR_MGMT_EXP_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["LABOR_MGMT_EXP_SCORE"],
                        comp_store.scores["ESRM_Flags"].get(
                            "WATER_STRESS_EXP_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["WATER_STRESS_EXP_SCORE"],
                        comp_store.scores["ESRM_Flags"].get(
                            "ENERGY_EFFICIENCY_EXP_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["ENERGY_EFFICIENCY_EXP_SCORE"],
                        comp_store.scores["ESRM_Flags"].get(
                            "BIODIV_LAND_USE_EXP_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["BIODIV_LAND_USE_EXP_SCORE"],
                        comp_store.scores["ESRM_Flags"].get(
                            "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL_Flag", 0
                        ),
                        comp_store.msci_information["CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL"],
                        comp_store.scores["ESRM_Flags"].get(
                            "CUSTOMER_RELATIONS_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["CUSTOMER_RELATIONS_SCORE"],
                        comp_store.scores["ESRM_Flags"].get(
                            "PROD_SFTY_QUALITY_EXP_SCORE_Flag", 0
                        ),
                        comp_store.msci_information["PROD_SFTY_QUALITY_EXP_SCORE"],
                        iva_rating,
                        comp_store.msci_information["OVERALL_FLAG"],
                        comp_store.msci_information["UNGC_COMPLIANCE"],
                        gov_score,
                        sum(comp_store.scores["Governance_Flags"].values()),
                        sum(comp_store.scores["NA_Flags_Governance"].values()),
                        comp_store.scores["Governance_Flags"].get(
                            "CARBON_EMISSIONS_CDP_DISCLOSURE_Flag", 0
                        ),
                        comp_store.msci_information["CARBON_EMISSIONS_CDP_DISCLOSURE"],
                        comp_store.scores["Governance_Flags"].get(
                            "COMBINED_CEO_CHAIR_Flag", 0
                        ),
                        comp_store.msci_information["COMBINED_CEO_CHAIR"],
                        comp_store.scores["Governance_Flags"].get(
                            "CONTROLLING_SHAREHOLDER_Flag", 0
                        ),
                        comp_store.msci_information["CONTROLLING_SHAREHOLDER"],
                        comp_store.scores["Governance_Flags"].get(
                            "CROSS_SHAREHOLDINGS_Flag", 0
                        ),
                        comp_store.msci_information["CROSS_SHAREHOLDINGS"],
                        comp_store.scores["Governance_Flags"].get(
                            "FEMALE_DIRECTORS_PCT_Flag", 0
                        ),
                        comp_store.msci_information["FEMALE_DIRECTORS_PCT"],
                        comp_store.scores["Governance_Flags"].get(
                            "INDEPENDENT_BOARD_MAJORITY_Flag", 0
                        ),
                        comp_store.msci_information["INDEPENDENT_BOARD_MAJORITY"],
                        comp_store.scores["Governance_Flags"].get(
                            "NEGATIVE_DIRECTOR_VOTES_Flag", 0
                        ),
                        comp_store.msci_information["NEGATIVE_DIRECTOR_VOTES"],
                        comp_store.scores["Governance_Flags"].get(
                            "PAY_LINKED_TO_SUSTAINABILITY_Flag", 0
                        ),
                        comp_store.msci_information["PAY_LINKED_TO_SUSTAINABILITY"],
                        comp_store.scores["Governance_Flags"].get(
                            "POISON_PILL_Flag", 0
                        ),
                        comp_store.msci_information["POISON_PILL"],
                        comp_store.scores["Governance_Flags"].get(
                            "WOMEN_EXEC_MGMT_PCT_RECENT_Flag", 0
                        ),
                        comp_store.msci_information["WOMEN_EXEC_MGMT_PCT_RECENT"],
                        trans_score,
                        comp_store.information.get("Carbon Intensity (Scope 123)", 0),
                        comp_store.sdg_information["ClimateGHGReductionTargets"],
                        comp_store.msci_information["HAS_SBTI_APPROVED_TARGET"],
                        comp_store.msci_information["HAS_COMMITTED_TO_SBTI_TARGET"],
                        comp_store.information.get("CapEx", 0),
                        comp_store.information.get("Climate_Revenue", 0),
                        comp_store.information.get("RENEWENERGY_MSCI", 0),
                        comp_store.information.get("RENEWENERGY_ISS", 0),
                        comp_store.information.get("MOBILITY_MSCI", 0),
                        comp_store.information.get("MOBILITY_ISS", 0),
                        comp_store.information.get("CIRCULARITY_MSCI", 0),
                        comp_store.information.get("CIRCULARITY_ISS", 0),
                        comp_store.information.get("CCADAPT_MSCI", 0),
                        comp_store.information.get("CCADAPT_ISS", 0),
                        comp_store.information.get("BIODIVERSITY_MSCI", 0),
                        comp_store.information.get("BIODIVERSITY_ISS", 0),
                        comp_store.information.get("SMARTCITIES_MSCI", 0),
                        comp_store.information.get("SMARTCITIES_ISS", 0),
                        comp_store.information.get("EDU_MSCI", 0),
                        comp_store.information.get("EDU_ISS", 0),
                        comp_store.information.get("HEALTH_MSCI", 0),
                        comp_store.information.get("HEALTH_ISS", 0),
                        comp_store.information.get("SANITATION_MSCI", 0),
                        comp_store.information.get("SANITATION_ISS", 0),
                        comp_store.information.get("INCLUSION_MSCI", 0),
                        comp_store.information.get("INCLUSION_ISS", 0),
                        comp_store.information.get("NUTRITION_MSCI", 0),
                        comp_store.information.get("NUTRITION_ISS", 0),
                        comp_store.information.get("AFFORDABLE_MSCI", 0),
                        comp_store.information.get("AFFORDABLE_ISS", 0),
                        risk_score_overall,
                    )
                )

    columns_detailed = [
        "Portfolio ISIN",
        "Portfolio Name",
        "Security ISIN",
        "Issuer Name",
        "Issuer Country",
        "Portfolio Weight",
        "OAS",
        "CARBON_EMISSIONS_SCOPE_12_INTEN",
        "Labeled ESG Type",
        "Sector Level 2",
        "BCLASS",
        "BCLASS_SECTOR",
        "GICS",
        "GICS_SECTOR",
        "SCLASS_Level1",
        "SCLASS_Level2",
        "SCLASS_Level3",
        "SCLASS_Level4",
        "SCLASS_Level4-P",
        "SCLASS-Level5",
        "Review Flag",
        "Review Comments",
        "Muni Score",
        "Securitized Score",
        "Sovereign Score",
        "ESRM Score",
        "ESRM_flagged",
        "NA_Flags_ESRM",
        "PRIVACY_DATA_SEC_EXP_SCORE_Flag",
        "PRIVACY_DATA_SEC_EXP_SCORE",
        "COMM_REL_RISK_EXP_SCORE_Flag",
        "COMM_REL_RISK_EXP_SCORE",
        "LABOR_MGMT_EXP_SCORE_Flag",
        "LABOR_MGMT_EXP_SCORE",
        "WATER_STRESS_EXP_SCORE_Flag",
        "WATER_STRESS_EXP_SCORE",
        "ENERGY_EFFICIENCY_EXP_SCORE_Flag",
        "ENERGY_EFFICIENCY_EXP_SCORE",
        "BIODIV_LAND_USE_EXP_SCORE_Flag",
        "BIODIV_LAND_USE_EXP_SCORE",
        "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL_Flag",
        "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL",
        "CUSTOMER_RELATIONS_SCORE_Flag",
        "CUSTOMER_RELATIONS_SCORE",
        "PROD_SFTY_QUALITY_EXP_SCORE_Flag",
        "PROD_SFTY_QUALITY_EXP_SCORE",
        "IVA_COMPANY_RATING",
        "OVERALL_FLAG",
        "UNGC_COMPLIANCE",
        "Governance Score",
        "Governance_flagged",
        "NA_Flags_Governance",
        "CARBON_EMISSIONS_CDP_DISCLOSURE_Flag",
        "CARBON_EMISSIONS_CDP_DISCLOSURE",
        "COMBINED_CEO_CHAIR_Flag",
        "COMBINED_CEO_CHAIR",
        "CONTROLLING_SHAREHOLDER_Flag",
        "CONTROLLING_SHAREHOLDER",
        "CROSS_SHAREHOLDINGS_Flag",
        "CROSS_SHAREHOLDINGS",
        "FEMALE_DIRECTORS_PCT_Flag",
        "FEMALE_DIRECTORS_PCT",
        "INDEPENDENT_BOARD_MAJORITY_Flag",
        "INDEPENDENT_BOARD_MAJORITY",
        "NEGATIVE_DIRECTOR_VOTES_Flag",
        "NEGATIVE_DIRECTOR_VOTES",
        "PAY_LINKED_TO_SUSTAINABILITY_Flag",
        "PAY_LINKED_TO_SUSTAINABILITY",
        "POISON_PILL_Flag",
        "POISON_PILL",
        "WOMEN_EXEC_MGMT_PCT_RECENT_Flag",
        "WOMEN_EXEC_MGMT_PCT_RECENT",
        "Transition Score",
        "Carbon Intensity (Scope 123)",
        "ClimateGHGReductionTargets",
        "HAS_SBTI_APPROVED_TARGET",
        "HAS_COMMITTED_TO_SBTI_TARGET",
        "CapEx",
        "Climate_Revenue",
        "RENEWENERGY_MSCI",
        "RENEWENERGY_ISS",
        "MOBILITY_MSCI",
        "MOBILITY_ISS",
        "CIRCULARITY_MSCI",
        "CIRCULARITY_ISS",
        "CCADAPT_MSCI",
        "CCADAPT_ISS",
        "BIODIVERSITY_MSCI",
        "BIODIVERSITY_ISS",
        "SMARTCITIES_MSCI",
        "SMARTCITIES_ISS",
        "EDU_MSCI",
        "EDU_ISS",
        "HEALTH_MSCI",
        "HEALTH_ISS",
        "SANITATION_MSCI",
        "SANITATION_ISS",
        "INCLUSION_MSCI",
        "INCLUSION_ISS",
        "NUTRITION_MSCI",
        "NUTRITION_ISS",
        "AFFORDABLE_MSCI",
        "AFFORDABLE_ISS",
        "Risk_Score_Overall",
    ]
    df_detailed = pd.DataFrame(data_detail, columns=columns_detailed)
    return df_detailed


def sector_subset(gics_list, bclass_list) -> pd.DataFrame:
    """
    Run risk framework and return scores for specific industries

    Parameters
    ----------
    gics_list: list
        list of GICS_SUB_IND
    bclass_list: list
        list of BCLASS_LEVEL4

    Returns
    -------
    pd.DataFrame
        Summary DataFrame for specific industries
    """
    r = runner.Runner()
    r.init()
    r.run()
    data = []
    for c in r.portfolio_datasource.companies:
        comp_store = r.portfolio_datasource.companies[c]
        issuer_name = comp_store.msci_information["ISSUER_NAME"]
        r_flag = comp_store.scores["Review_Flag"]
        r_comments = comp_store.scores["Review_Comments"]
        s2 = comp_store.information["Sector_Level_2"]
        bclass = comp_store.information["BCLASS_Level4"].class_name
        gics = comp_store.information["GICS_SUB_IND"].class_name
        esrm_score = comp_store.scores["ESRM_Score"]
        gov_score = comp_store.scores["Governance_Score"]
        trans_score = comp_store.scores["Transition_Score"]
        for s in comp_store.securities:
            sec_store = r.companies[c].securities[s]
            level_1 = sec_store.information["SClass_Level1"]
            level_2 = sec_store.information["SClass_Level2"]
            level_3 = sec_store.information["SClass_Level3"]
            level_4 = sec_store.information["SClass_Level4"]
            level_4p = sec_store.information["SClass_Level4-P"]
            risk_score_overall = sec_store.scores["Risk_Score_Overall"]

            if bclass in bclass_list or gics in gics_list:
                data.append(
                    (
                        s,
                        issuer_name,
                        r_flag,
                        r_comments,
                        s2,
                        bclass,
                        gics,
                        esrm_score,
                        gov_score,
                        risk_score_overall,
                        trans_score,
                        level_1,
                        level_2,
                        level_3,
                        level_4,
                        level_4p,
                    )
                )

    columns = [
        "Security ISIN",
        "Issuer Name",
        "Review Flag",
        "Review Comments",
        "Sector Level 2",
        "BCLASS",
        "GICS",
        "ESRM Score",
        "Governance Score",
        "Risk_Score_Overall",
        "Transition Score",
        "SCLASS_Level1",
        "SCLASS_Level2",
        "SCLASS_Level3",
        "SCLASS_Level4",
        "SCLASS_Level4-P",
    ]
    df = pd.DataFrame(data, columns=columns)
    return df


def isin_lookup(isin_list: list) -> pd.DataFrame:
    """
    For a list of ISINs, run the risk framework

    Parameters
    ----------
    isin_list: list
        list of isins

    Returns
    -------
    pd.DataFrame
        Summary DataFrame with score data for entered isins
    """

    params = configs.read_configs()

    # create portfolio sheet
    portfolio_df = pd.DataFrame()
    portfolio_df["ISIN"] = isin_list
    portfolio_df["As Of Date"] = pd.to_datetime("today").normalize()
    portfolio_df["Portfolio"] = "Test_Portfolio"
    portfolio_df["Portfolio Name"] = "Test_Portfolio"
    portfolio_df["ESG Collateral Type"] = "Unknown"
    portfolio_df["Issuer ESG"] = "No"
    portfolio_df["Labeled ESG Type"] = "None"
    portfolio_df["ISSUER_NAME"] = isin_list
    portfolio_df["TCW ESG"] = "None"
    portfolio_df["Ticker Cd"] = np.nan
    portfolio_df["Sector Level 1"] = "Corporate"
    portfolio_df["Sector Level 2"] = "Industrial"
    portfolio_df["BCLASS_Level4"] = np.nan
    portfolio_df["Portfolio_Weight"] = 1 / len(isin_list)
    portfolio_df["Base Mkt Val"] = 1
    portfolio_df["Rating Raw MSCI"] = np.nan
    portfolio_df["OAS"] = np.nan
    portfolio_df = portfolio_df.to_json(orient="index")

    # create msci mapping file
    msci_df = data_loaders.create_msci_mapping(isin_list=isin_list, params=params)
    msci_df = msci_df.to_json(orient="index")

    configs_overwrite = {
        "portfolio_datasource": {"source": 6, "json_str": portfolio_df, "load": True},
        "security_datasource": {
            "iss": {
                "source": 2,
                "file": "C:/Users/bastit/Documents/Risk_Score/Input/Multi-Security_Standard_Issuers_20230503.csv",
                "load": True,
            },
            "msci": {"source": 6, "json_str": msci_df, "load": True},
        },
    }
    with open(params["configs_path"], "w") as f:
        json.dump(configs_overwrite, f)

    # run framework
    r = runner.Runner()
    r.init()
    r.run()
    data = []
    for p in r.portfolio_datasource.portfolios:
        portfolio_isin = r.portfolio_datasource.portfolios[p].id
        portfolio_name = r.portfolio_datasource.portfolios[p].name
        for s in r.portfolio_datasource.portfolios[p].holdings:
            sec_store = r.portfolio_datasource.portfolios[p].holdings[s]["object"]
            comp_store = sec_store.parent_store
            issuer_name = sec_store.information["IssuerName"]
            ticker = comp_store.msci_information["ISSUER_TICKER"]
            iva_rating = comp_store.information["IVA_COMPANY_RATING"]
            s2 = comp_store.information["Sector_Level_2"]
            bclass = comp_store.information["BCLASS_Level4"].class_name
            gics = comp_store.information["GICS_SUB_IND"].class_name
            muni_score = comp_store.scores["Muni_Score"]
            sec_score = sec_store.scores["Securitized_Score"]
            sov_score = comp_store.scores["Sovereign_Score"]
            esrm_score = comp_store.scores["ESRM_Score"]
            na_esrm = sum(comp_store.scores["NA_Flags_ESRM"].values())
            gov_score = comp_store.scores["Governance_Score"]
            na_gov = sum(comp_store.scores["NA_Flags_Governance"].values())
            trans_score = comp_store.scores["Transition_Score"]
            level_1 = sec_store.information["SClass_Level1"]
            level_2 = sec_store.information["SClass_Level2"]
            level_3 = sec_store.information["SClass_Level3"]
            level_4 = sec_store.information["SClass_Level4"]
            level_4p = sec_store.information["SClass_Level4-P"]
            risk_score_overall = sec_store.scores["Risk_Score_Overall"]
            labeled_esg_type = sec_store.information["Labeled_ESG_Type"]

            if (
                portfolio_isin in r.params["A8Funds"]
                and "Article 8" in comp_store.information["Exclusion"]
                and not (
                    labeled_esg_type
                    in [
                        "Labeled Green",
                        "Labeled Social",
                        "Labeled Sustainable",
                        "Labeled Sustainable Linked",
                    ]
                    and bclass in r.params["carve_out_sectors"]
                )
            ):
                level_1 = "Excluded"
                level_2 = "Exclusion"
                level_3 = "Exclusion"
                level_4 = "Excluded Sector"
                level_4p = "Excluded Sector"
            elif (
                portfolio_isin in r.params["A9Funds"]
                and "Article 9" in comp_store.information["Exclusion"]
                and not (
                    labeled_esg_type
                    in [
                        "Labeled Green",
                        "Labeled Social",
                        "Labeled Sustainable",
                        "Labeled Sustainable Linked",
                    ]
                    and bclass in r.params["carve_out_sectors"]
                )
            ):
                level_1 = "Excluded"
                level_2 = "Exclusion"
                level_3 = "Exclusion"
                level_4 = "Excluded Sector"
                level_4p = "Excluded Sector"

            if "INTELSAT" in sec_store.information["Security_Name"]:
                level_1 = "Preferred"
                level_2 = "Sustainable Theme"
                level_3 = "People"
                level_4 = "INCLUSION"
                level_4p = "INCLUSION"
                gov_score = 4
                esrm_score = 1

            holding_measures = r.portfolio_datasource.portfolios[p].holdings[s][
                "holding_measures"
            ]
            for h in holding_measures:
                portfolio_weight = h["Portfolio_Weight"]
                oas = h["OAS"]
                data.append(
                    (
                        portfolio_isin,
                        portfolio_name,
                        s,
                        issuer_name,
                        ticker,
                        iva_rating,
                        s2,
                        bclass,
                        gics,
                        portfolio_weight,
                        oas,
                        muni_score,
                        sec_score,
                        sov_score,
                        esrm_score,
                        na_esrm,
                        gov_score,
                        na_gov,
                        trans_score,
                        risk_score_overall,
                        level_1,
                        level_2,
                        level_3,
                        level_4,
                        level_4p,
                        comp_store.information.get("RENEWENERGY_MSCI", 0),
                        comp_store.information.get("RENEWENERGY_ISS", 0),
                        comp_store.information.get("MOBILITY_MSCI", 0),
                        comp_store.information.get("MOBILITY_ISS", 0),
                        comp_store.information.get("CIRCULARITY_MSCI", 0),
                        comp_store.information.get("CIRCULARITY_ISS", 0),
                        comp_store.information.get("CCADAPT_MSCI", 0),
                        comp_store.information.get("CCADAPT_ISS", 0),
                        comp_store.information.get("BIODIVERSITY_MSCI", 0),
                        comp_store.information.get("BIODIVERSITY_ISS", 0),
                        comp_store.information.get("SMARTCITIES_MSCI", 0),
                        comp_store.information.get("SMARTCITIES_ISS", 0),
                        comp_store.information.get("EDU_MSCI", 0),
                        comp_store.information.get("EDU_ISS", 0),
                        comp_store.information.get("HEALTH_MSCI", 0),
                        comp_store.information.get("HEALTH_ISS", 0),
                        comp_store.information.get("SANITATION_MSCI", 0),
                        comp_store.information.get("SANITATION_ISS", 0),
                        comp_store.information.get("INCLUSION_MSCI", 0),
                        comp_store.information.get("INCLUSION_ISS", 0),
                        comp_store.information.get("NUTRITION_MSCI", 0),
                        comp_store.information.get("NUTRITION_ISS", 0),
                        comp_store.information.get("AFFORDABLE_MSCI", 0),
                        comp_store.information.get("AFFORDABLE_ISS", 0),
                    )
                )

    columns = [
        "Portfolio ISIN",
        "Portfolio Name",
        "Security ISIN",
        "Issuer Name",
        "Ticker",
        "IVA_COMPANY_RATING",
        "Sector Level 2",
        "BCLASS",
        "GICS",
        "Portfolio Weight",
        "OAS",
        "Muni Score",
        "Securitized Score",
        "Sovereign Score",
        "ESRM Score",
        "NA_Flags_ESRM",
        "Governance Score",
        "NA_Flags_Governance",
        "Transition Score",
        "Risk Overall Score",
        "SCLASS_Level1",
        "SCLASS_Level2",
        "SCLASS_Level3",
        "SCLASS_Level4",
        "SCLASS_Level4-P",
        "RENEWENERGY_MSCI",
        "RENEWENERGY_ISS",
        "MOBILITY_MSCI",
        "MOBILITY_ISS",
        "CIRCULARITY_MSCI",
        "CIRCULARITY_ISS",
        "CCADAPT_MSCI",
        "CCADAPT_ISS",
        "BIODIVERSITY_MSCI",
        "BIODIVERSITY_ISS",
        "SMARTCITIES_MSCI",
        "SMARTCITIES_ISS",
        "EDU_MSCI",
        "EDU_ISS",
        "HEALTH_MSCI",
        "HEALTH_ISS",
        "SANITATION_MSCI",
        "SANITATION_ISS",
        "INCLUSION_MSCI",
        "INCLUSION_ISS",
        "NUTRITION_MSCI",
        "NUTRITION_ISS",
        "AFFORDABLE_MSCI",
        "AFFORDABLE_ISS",
    ]

    df = pd.DataFrame(data, columns=columns)

    with open(params["configs_path"], "w") as f:
        json.dump({}, f)
    return df
