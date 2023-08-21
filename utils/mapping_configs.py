import importlib
import numpy as np
import quantkit.finance.companies.munis as munis
import quantkit.finance.companies.securitized as securitized
import quantkit.finance.companies.sovereigns as sovereigns
import quantkit.finance.sectors.sectors as sectors

security_store_mapping = {
    "Equity": getattr(
        importlib.import_module("quantkit.finance.securities.securities"), "EquityStore"
    ),
    "Fixed Income": getattr(
        importlib.import_module("quantkit.finance.securities.securities"),
        "FixedIncomeStore",
    ),
    " ": getattr(
        importlib.import_module("quantkit.finance.securities.securities"),
        "SecurityStore",
    ),
    np.nan: getattr(
        importlib.import_module("quantkit.finance.securities.securities"),
        "SecurityStore",
    ),
}

security_mapping = {
    "Equity": "equities",
    "Fixed Income": "fixed_income",
    " ": "other_securities",
    np.nan: "other_securities",
}


security_type_mapping = {
    "Securitized": securitized.SecuritizedStore,
    "Muni": munis.MuniStore,
    "Sovereign": sovereigns.SovereignStore,
}

sector_mapping = {"BCLASS_Level4": sectors.BClass, "GICS_SUB_IND": sectors.GICS}

TargetA = ["Approved SBT"]
TargetAA = ["Approved SBT", "Ambitious Target"]
TargetAAC = ["Approved SBT", "Committed SBT", "Ambitious Target"]
TargetAACN = [
    "Approved SBT",
    "Committed SBT",
    "Ambitious Target",
    "Non-Ambitious Target",
]
TargetAC = ["Approved SBT", "Committed SBT"]
TargetCA = ["Committed SBT", "Ambitious Target"]
TargetCN = ["Committed SBT", "Non-Ambitious Target"]
TargetN = ["Non-Ambitious Target"]

exclusions = {
    "AlcProDistMaxRevPctAgg": "Alcohol",
    "TobMaxRevPct": "Tobacco",
    "UnconvOilGasMaxRevPct": "Oil_Gas",
    "OgRev": "Oil_Gas",
    "GamMaxRevPct": "Gambling",
    "FirearmMaxRevPct": "Weapons_Firearms",
    "WeapMaxRevPct": "Weapons_Firearms",
    "ThermalCoalMaxRevPct": "Thermal_Coal_Mining",
    "GeneratMaxRevThermalCoal": "Thermal_Coal_Power_Gen",
    "CweapTie": "Controversial_Weapons",
    "HrCompliance": "UN_Alignement",
    "UngcCompliance": "UN_Alignement",
    "AeMaxRevPct": "Adult_Entertainment",
    "IVA_COMPANY_RATING": "ESG_Rating",
}

pai_mapping = {
    "CARBON_EMISSIONS_SCOPE_1": "1.1 - Scope 1 GHG Emissions",
    "CARBON_EMISSIONS_SCOPE_2": "1.2 - Scope 2 GHG Emissions",
    "CARBON_EMISSIONS_SCOPE_3_TOTAL": "1.3 - Scope 3 GHG Emissions",
    "CARBON_EMISSIONS_SCOPE123": "1.4 - Total GHG Emissions (Scope 1,2,3)",
    "Carbon_Footprint": "2 - Carbon Footprint (Scope 1+2+3)",
    "WACI": "3 - Weighted Average Carbon Intensity (Scope 1+2+3)",
    "Fossil_Fuel_Exposure": "4 - Fossil Fuel Exposure",
    "PCT_NONRENEW_CONSUMP_PROD": "5 - Non-Renewable Energy Consumption and Production",
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
    "FEMALE_DIRECTORS_PCT": "13 - Board Gender Diversity",
    "CONTRO_WEAP_CBLMBW_ANYTIE": "14 - Exposure to Controversial Weapons",
    "CTRY_GHG_INTEN_GDP_EUR": "15 - GHG Intensity of Investee Countries",
    "SANCTIONS": "16 - Investee Countries Subject to Social Violations",
    "FOSSIL_FUELS_REAL_ESTATE": "17 - Exposure to fossil fuels through real estate assets",
    "ENERGY_INEFFICIENT_REAL_ESTATE": "18 - Exposure to energy-inefficient real estate assets",
    "CARBON_EMISSIONS_REDUCT_INITIATIVES": "Additional Environmental - Investment in Companies w/o Carbon Emissions Reduction Targets",
    "WORKPLACE_ACC_PREV_POL": "Additional Social - No Workplace Accident Prevention Policy",
}
