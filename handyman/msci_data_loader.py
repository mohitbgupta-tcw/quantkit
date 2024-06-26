import pandas as pd
import quantkit.core.data_sources.msci as msci
import quantkit.utils.configs as configs


def create_msci_mapping(
    issuer_identifier_type: str, issuer_identifier_list: list, local_configs: str = ""
) -> pd.DataFrame:
    """
    Create MSCI mapping DataFrame

    Parameters
    ----------
    issuer_identifier_type: str
        Issuer Identifier Type, should either be 'ISIN' or 'ISSUERID'
    issuer_identifier_list
        list of identifiers of corresponding type
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
    """
    params = configs.read_configs(local_configs=local_configs)
    msci_params = params["API_settings"]["msci_parameters"]
    filters = {
        "issuer_identifier_type": issuer_identifier_type,
        "issuer_identifier_list": issuer_identifier_list,
        "parent_child": "inherit_missing_values",
        "factor_name_list": [
            "ISSUER_NAME",
            "ISSUER_TICKER",
            "ISSUER_CUSIP",
            "ISSUER_SEDOL",
            "ISSUER_ISIN",
            "ISSUER_CNTRY_DOMICILE",
            "ISSUERID",
        ],
    }
    url = "https://api.msci.com/esg/data/v2.0/issuers?category_path_list=ESG+Ratings:Company+Summary&coverage=esg_ratings&format=json"
    msci_object = msci.MSCI(url=url, filters=filters, **msci_params)
    msci_object.load()
    msci_df = msci_object.df
    return msci_df


def run_msci_api(
    issuer_identifier_type: str,
    issuer_identifier_list: list,
    factor_name_list: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load MSCI ESG data through API

    Parameters
    ----------
    issuer_identifier_type: str
        Issuer Identifier Type, should either be 'ISIN' or 'ISSUERID'
    issuer_identifier_list
        list of identifiers of corresponding type
    factor_name_list: list, optional
        factors to pull from MSCI, if not specified pull pre-defined data

    Returns
    -------
    pd.DataFrame
        DataFrame of MSCI information
    """

    api_key = "B0ULIIFQM0qFl50yaT9AFB0M3sbhswcg"
    api_secret = "D9_8FoQlTeFkGTjI7RkProVa2M5z9SI2xAm4wz6WLsiW9_h2DVZKhqtSuxPcH5OZ"

    if not factor_name_list:
        factor_name_list = [
            "ISSUER_NAME",
            "ISSUER_TICKER",
            "ISSUER_CUSIP",
            "ISSUER_SEDOL",
            "ISSUER_ISIN",
            "ISSUER_CNTRY_DOMICILE",
            "CT_ALT_ENERGY_BIOFUEL_MAX_REV",
            "CT_ALT_ENERGY_BIOMASS_MAX_REV",
            "CT_ALT_ENERGY_FUEL_CELLS_MAX_REV",
            "CT_ALT_ENERGY_GAS_COGEN_MAX_REV",
            "CT_ALT_ENERGY_GEOTHERMAL_MAX_REV",
            "CT_ALT_ENERGY_MAX_REV",
            "CT_ALT_ENERGY_RENEWABLE_POWER_EQUIP_MAX_REV",
            "CT_ALT_ENERGY_RENEWABLE_POWER_GEN_MAX_REV",
            "CT_ALT_ENERGY_SMALL_HYDRO_MAX_REV",
            "CT_ALT_ENERGY_SOLAR_MAX_REV",
            "CT_ALT_ENERGY_STORAGE_MAX_REV",
            "CT_ALT_ENERGY_WASTE_TO_ENERGY_MAX_REV",
            "CT_ALT_ENERGY_WAVE_TIDAL_MAX_REV",
            "CT_ALT_ENERGY_WIND_MAX_REV",
            "CT_ENERGY_EFF_CLN_TRAN_INFRA_MAX_REV",
            "CT_ENERGY_EFF_DEMAND_MGMT_MAX_REV",
            "CT_ENERGY_EFF_ELECTRIC_VEH_MAX_REV",
            "CT_ENERGY_EFF_HYBRID_VEH_MAX_REV",
            "CT_ENERGY_EFF_INDUSTRL_AUTOM_MAX_REV",
            "CT_ENERGY_EFF_INSULATION_MAX_REV",
            "CT_ENERGY_EFF_LED_CFL_MAX_REV",
            "CT_ENERGY_EFF_MAX_REV",
            "CT_ENERGY_EFF_OPT_INFRA_MAX_REV",
            "CT_ENERGY_EFF_SMART_GRID_MAX_REV",
            "CT_ENERGY_EFF_SUPERCONDUCTORS_MAX_REV",
            "CT_ENERGY_EFF_ZERO_EM_VEH_MAX_REV",
            "CT_GREEN_BLDG_CONSTR_MAX_REV",
            "CT_GREEN_BLDG_MAX_REV",
            "CT_CC_TOTAL_MAX_REV",
            "CT_GREEN_BLDG_PROP_MGMT_MAX_REV",
            "CT_POLL_PREV_CONV_POLL_CTRL_MAX_REV",
            "CT_POLL_PREV_LOW_TOX_VOC_MAX_REV",
            "CT_POLL_PREV_MAX_REV",
            "CT_POLL_PREV_RECYCLING_MAX_REV",
            "CT_POLL_PREV_REMEDIATION_MAX_REV",
            "CT_SUST_AG_MAX_REV",
            "CT_SUST_WATER_DESALINIZATION_MAX_REV",
            "CT_SUST_WATER_DROUGHT_RES_SEEDS_MAX_REV",
            "CT_SUST_WATER_INFRA_DISTRIB_MAX_REV",
            "CT_SUST_WATER_MAX_REV",
            "CT_SUST_WATER_RAINWATER_MAX_REV",
            "CT_SUST_WATER_RECYCLING_MAX_REV",
            "CT_SUST_WATER_SMART_METER_MAX_REV",
            "CT_SUST_WATER_WASTE_WATER_TRTMT_MAX_REV",
            "CT_TOTAL_MAX_REV",
            "SI_EDU_SERV_MAX_REV",
            "SI_HEALTHCARE_MAX_REV",
            "SI_ORPHAN_DRUGS_REV",
            "SI_SANITARY_PROD_MAX_REV",
            "CT_POLL_PREV_WASTE_TREATMENT_MAX_REV",
            "SI_SOCIAL_FIN__MAX_REV",
            "SI_NUTRI_FOOD_MAX_REV",
            "SI_AFFORD_RE_MAX_REV",
            "SI_EMPOWER_TOTAL_MAX_REV",
            "SI_BASIC_N_TOTAL_MAX_REV",
            "GICS_SUB_IND",
            "RENEW_ENERGY_CAPEX_VS_TOTAL_CAPEX_PCT",
            "SI_CONNECTIVITY_MAX_REV",
            "HAS_SBTI_APPROVED_TARGET",
            "HAS_COMMITTED_TO_SBTI_TARGET",
            "THERMAL_COAL_MAX_REV_PCT",
            "CONTROLLING_SHAREHOLDER",
            "CROSS_SHAREHOLDINGS",
            "POISON_PILL",
            "COMBINED_CEO_CHAIR",
            "INDEPENDENT_BOARD_MAJORITY",
            "NEGATIVE_DIRECTOR_VOTES",
            "WOMEN_DIRECTORS_PCT_RECENT",
            "WOMEN_EXEC_MGMT_PCT_RECENT",
            "PAY_LINKED_TO_SUSTAINABILITY",
            "CARBON_EMISSIONS_CDP_DISCLOSURE",
            "FEMALE_DIRECTORS_PCT",
            "SALES_USD_RECENT",
            "CARBON_EMISSIONS_SCOPE123",
            "COMPANY_REV_USD_RECENT",
            "CONTROVERSY_FLAG",
            "UNGC_COMPLIANCE",
            "ALC_MAX_REV",
            "ARCTIC_OIL_MAX_REV_PCT",
            "ARCTIC_GAS_MAX_REV_PCT",
            "PRIVACY_DATA_SEC_EXP_SCORE",
            "COMM_REL_RISK_EXP_SCORE",
            "LABOR_MGMT_EXP_SCORE",
            "WATER_STRESS_EXP_SCORE",
            "ENERGY_EFFICIENCY_EXP_SCORE",
            "BIODIV_LAND_USE_EXP_SCORE",
            "CUSTOMER_RELATIONS_SCORE",
            "PROD_SFTY_QUALITY_EXP_SCORE",
            "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL",
            "IVA_COMPANY_RATING",
            "OVERALL_FLAG",
            "ISSUERID",
            "PARENT_ISSUERID",
            "PARENT_ULTIMATE_ISSUERID",
            "PALM_TIE",
            "ACCESS_TO_HLTHCRE_SCORE",
            "CARBON_EMISSIONS_SCOPE_12_INTEN",
            "CARBON_GOVERNMENT_GHG_INTENSITY_GDP_TONPERMN",
        ]

    filters = {
        "issuer_identifier_type": issuer_identifier_type,
        "issuer_identifier_list": issuer_identifier_list,
        "parent_child": "inherit_missing_values",
        "factor_name_list": factor_name_list,
    }

    url = "https://api.msci.com/esg/data/v2.0/issuers?category_path_list=ESG+Ratings:Company+Summary&coverage=esg_ratings&format=json"

    msci_object = msci.MSCI(api_key, api_secret, url, filters)
    msci_object.load()
    return msci_object.df


def run_historical_msci_api(
    issuer_identifier_type: str,
    issuer_identifier_list: list,
    start_date: str,
    end_date: str,
    as_at_date: str,
    factor_name_list: list = None,
) -> pd.DataFrame:
    """
    For a specified list of securities, load historical MSCI ESG data through API

    Parameters
    ----------
    issuer_identifier_type: str
        Issuer Identifier Type, should either be 'ISIN' or 'ISSUERID'
    issuer_identifier_list
        list of identifiers of corresponding type
    start_date: str
        start date in format yyyy-mm-dd
    end_date: str
        end range values are returned for in format yyyy-mm-dd
        if not specified, request will return single value corresponding to start_date
    as_at_date: str
        point in time data should be viewed from in format yyyy-mm-dd
    factor_name_list: list, optional
        factors to pull from MSCI, if not specified pull pre-defined data

    Returns
    -------
    pd.DataFrame
        DataFrame of MSCI information
    """

    api_key = "B0ULIIFQM0qFl50yaT9AFB0M3sbhswcg"
    api_secret = "D9_8FoQlTeFkGTjI7RkProVa2M5z9SI2xAm4wz6WLsiW9_h2DVZKhqtSuxPcH5OZ"

    if not factor_name_list:
        factor_name_list = [
            "ISSUER_NAME",
            "ISSUER_TICKER",
            "ISSUER_CUSIP",
            "ISSUER_SEDOL",
            "ISSUER_ISIN",
            "ISSUER_CNTRY_DOMICILE",
            "CT_ALT_ENERGY_BIOFUEL_MAX_REV",
            "CT_ALT_ENERGY_BIOMASS_MAX_REV",
            "CT_ALT_ENERGY_FUEL_CELLS_MAX_REV",
            "CT_ALT_ENERGY_GAS_COGEN_MAX_REV",
            "CT_ALT_ENERGY_GEOTHERMAL_MAX_REV",
            "CT_ALT_ENERGY_MAX_REV",
            "CT_ALT_ENERGY_RENEWABLE_POWER_EQUIP_MAX_REV",
            "CT_ALT_ENERGY_RENEWABLE_POWER_GEN_MAX_REV",
            "CT_ALT_ENERGY_SMALL_HYDRO_MAX_REV",
            "CT_ALT_ENERGY_SOLAR_MAX_REV",
            "CT_ALT_ENERGY_STORAGE_MAX_REV",
            "CT_ALT_ENERGY_WASTE_TO_ENERGY_MAX_REV",
            "CT_ALT_ENERGY_WAVE_TIDAL_MAX_REV",
            "CT_ALT_ENERGY_WIND_MAX_REV",
            "CT_ENERGY_EFF_CLN_TRAN_INFRA_MAX_REV",
            "CT_ENERGY_EFF_DEMAND_MGMT_MAX_REV",
            "CT_ENERGY_EFF_ELECTRIC_VEH_MAX_REV",
            "CT_ENERGY_EFF_HYBRID_VEH_MAX_REV",
            "CT_ENERGY_EFF_INDUSTRL_AUTOM_MAX_REV",
            "CT_ENERGY_EFF_INSULATION_MAX_REV",
            "CT_ENERGY_EFF_LED_CFL_MAX_REV",
            "CT_ENERGY_EFF_MAX_REV",
            "CT_ENERGY_EFF_OPT_INFRA_MAX_REV",
            "CT_ENERGY_EFF_SMART_GRID_MAX_REV",
            "CT_ENERGY_EFF_SUPERCONDUCTORS_MAX_REV",
            "CT_ENERGY_EFF_ZERO_EM_VEH_MAX_REV",
            "CT_GREEN_BLDG_CONSTR_MAX_REV",
            "CT_GREEN_BLDG_MAX_REV",
            "CT_CC_TOTAL_MAX_REV",
            "CT_GREEN_BLDG_PROP_MGMT_MAX_REV",
            "CT_POLL_PREV_CONV_POLL_CTRL_MAX_REV",
            "CT_POLL_PREV_LOW_TOX_VOC_MAX_REV",
            "CT_POLL_PREV_MAX_REV",
            "CT_POLL_PREV_RECYCLING_MAX_REV",
            "CT_POLL_PREV_REMEDIATION_MAX_REV",
            "CT_SUST_AG_MAX_REV",
            "CT_SUST_WATER_DESALINIZATION_MAX_REV",
            "CT_SUST_WATER_DROUGHT_RES_SEEDS_MAX_REV",
            "CT_SUST_WATER_INFRA_DISTRIB_MAX_REV",
            "CT_SUST_WATER_MAX_REV",
            "CT_SUST_WATER_RAINWATER_MAX_REV",
            "CT_SUST_WATER_RECYCLING_MAX_REV",
            "CT_SUST_WATER_SMART_METER_MAX_REV",
            "CT_SUST_WATER_WASTE_WATER_TRTMT_MAX_REV",
            "CT_TOTAL_MAX_REV",
            "SI_EDU_SERV_MAX_REV",
            "SI_HEALTHCARE_MAX_REV",
            "SI_ORPHAN_DRUGS_REV",
            "SI_SANITARY_PROD_MAX_REV",
            "CT_POLL_PREV_WASTE_TREATMENT_MAX_REV",
            "SI_SOCIAL_FIN__MAX_REV",
            "SI_NUTRI_FOOD_MAX_REV",
            "SI_AFFORD_RE_MAX_REV",
            "SI_EMPOWER_TOTAL_MAX_REV",
            "SI_BASIC_N_TOTAL_MAX_REV",
            "GICS_SUB_IND",
            "RENEW_ENERGY_CAPEX_VS_TOTAL_CAPEX_PCT",
            "SI_CONNECTIVITY_MAX_REV",
            "HAS_SBTI_APPROVED_TARGET",
            "HAS_COMMITTED_TO_SBTI_TARGET",
            "THERMAL_COAL_MAX_REV_PCT",
            "CONTROLLING_SHAREHOLDER",
            "CROSS_SHAREHOLDINGS",
            "POISON_PILL",
            "COMBINED_CEO_CHAIR",
            "INDEPENDENT_BOARD_MAJORITY",
            "NEGATIVE_DIRECTOR_VOTES",
            "WOMEN_DIRECTORS_PCT_RECENT",
            "WOMEN_EXEC_MGMT_PCT_RECENT",
            "PAY_LINKED_TO_SUSTAINABILITY",
            "CARBON_EMISSIONS_CDP_DISCLOSURE",
            "FEMALE_DIRECTORS_PCT",
            "SALES_USD_RECENT",
            "CARBON_EMISSIONS_SCOPE123",
            "COMPANY_REV_USD_RECENT",
            "CONTROVERSY_FLAG",
            "UNGC_COMPLIANCE",
            "ALC_MAX_REV",
            "ARCTIC_OIL_MAX_REV_PCT",
            "ARCTIC_GAS_MAX_REV_PCT",
            "PRIVACY_DATA_SEC_EXP_SCORE",
            "COMM_REL_RISK_EXP_SCORE",
            "LABOR_MGMT_EXP_SCORE",
            "WATER_STRESS_EXP_SCORE",
            "ENERGY_EFFICIENCY_EXP_SCORE",
            "BIODIV_LAND_USE_EXP_SCORE",
            "CUSTOMER_RELATIONS_SCORE",
            "PROD_SFTY_QUALITY_EXP_SCORE",
            "CONTR_SUPPLY_CHAIN_LABOR_N_TOTAL",
            "IVA_COMPANY_RATING",
            "OVERALL_FLAG",
            "ISSUERID",
            "PARENT_ISSUERID",
            "PARENT_ULTIMATE_ISSUERID",
            "PALM_TIE",
            "ACCESS_TO_HLTHCRE_SCORE",
            "CARBON_EMISSIONS_SCOPE_12_INTEN",
            "CARBON_GOVERNMENT_GHG_INTENSITY_GDP_TONPERMN",
        ]

    filters = {
        "issuer_identifier_type": issuer_identifier_type,
        "issuer_identifier_list": issuer_identifier_list,
        "inherit_missing_values": True,
        "factor_name_list": factor_name_list,
        "start_date": start_date,
        "end_date": end_date,
        "as_at_date": as_at_date,
        "data_sample_frequency": "business_month_end",
        "data_layout": "by_factor",
    }

    url = "https://api.msci.com/esg/data/v2.0/issuers/history?category_path_list=ESG+Ratings:Company+Summary&coverage=esg_ratings&format=json"

    msci_object = msci.MSCI(api_key, api_secret, url, filters)
    msci_object.load_historical()
    return msci_object.df
