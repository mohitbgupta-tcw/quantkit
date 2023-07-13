import pandas as pd
import quantkit.data_sources.msci as msci
import quantkit.data_sources.quandl as quandl


def create_msci_mapping(isin_list: list, params: dict) -> pd.DataFrame:
    """
    Create MSCI mapping DataFrame

    Parameters
    ----------
    isin_list: list
        list of isins
    params: dict
        parameters with msci information

    Returns
    -------
    pd.DataFrame
    """
    msci_params = params["msci_parameters"]
    filters = {
        "issuer_identifier_type": "ISIN",
        "issuer_identifier_list": isin_list,
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
    url = "https://api.msci.com/esg/data/v1.0/issuers?category_path_list=ESG+Ratings:Company+Summary&coverage=esg_ratings&format=json"
    msci_object = msci.MSCI(url=url, filters=filters, **msci_params)
    msci_object.load()
    msci_df = msci_object.df
    msci_df = msci_df.rename(columns={"CLIENT_IDENTIFIER": "Client_ID"})
    return msci_df


def run_msci_api(
    issuer_identifier_type: str, issuer_identifier_list: list
) -> pd.DataFrame:
    """
    For a specified list of securities, load MSCI ESG data through API

    Parameters
    ----------
    issuer_identifier_type: str
        Issuer Identifier Type, should either be 'ISIN' or 'ISSUERID'
    issuer_identifier_list
        list of identifiers of corresponding type

    Returns
    -------
    pd.DataFrame
        DataFrame of MSCI information
    """

    api_key = "b769e9c4-16c4-45f8-89a5-6f555587a640"
    api_secret = "0a7251dfd9dadd7f92444cab7e665e875a38ff5a"

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
            "OG_REV",
            "TOB_MAX_REV_PCT",
            "WEAP_MAX_REV_PCT",
            "GAM_MAX_REV_PCT",
            "ALC_MAX_REV",
            "ARCTIC_OIL_MAX_REV_PCT",
            "ARCTIC_GAS_MAX_REV_PCT",
            "UNCONV_OIL_GAS_MAX_REV_PCT",
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
        ],
    }

    url = "https://api.msci.com/esg/data/v1.0/issuers?category_path_list=ESG+Ratings:Company+Summary&coverage=esg_ratings&format=json"

    msci_object = msci.MSCI(api_key, api_secret, url, filters)
    msci_object.load()
    return msci_object.df


def run_quandl_api(ticker: list = ["AAPL", "MSFT", "TSLA"]) -> pd.DataFrame:
    """
    For a specified list of securities, load Quandl Fundamental data through API

    Parameters
    ----------
    ticker: list
        list of tickers to run in API

    Returns
    -------
    pd.DataFrame
        DataFrame of Quandl information
    """
    api_key = "MxE6oNePp886npLJ2CGs"
    table = "SHARADAR/SF1"
    filters = {
        "ticker": ticker,
        "dimension": "MRT",
        "calendardate": {"gte": "2023-01-01"},
        "paginate": True,
    }

    quandl_object = quandl.Quandl(api_key, table, filters)
    quandl_object.load()
    return quandl_object.df
