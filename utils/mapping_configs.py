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

portfolio_benchmark = {
    "3750": "JESG EMBI Global Diversified Index",
    "6739": "RUSSELL 1000 VALUE",
    "3720": "BLOOMBERG AGGREGATE INDEX",
    "3738": "BB HIGH YIELD 2% CAP",
    "3775": "Libor 3M",
    "13727": "Libor 3M",
    "13751": "Libor 3M",
    "16705": "RUSSELL 3000 GROWTH INDEX",
    "6717": "RUSSELL 1000 VALUE",
    "708": "US 1-3 Month T-Bill Index",
    "702": "BLOOMBERG AGGREGATE INDEX",
    "703": "S&P 500 INDEX",
    "704": "BB INTERMED GOVT/CRED",
    "705": "BB HIGH YIELD 2% CAP",
    "706": "MERRILL 3 M TBILL + 200bp",
    "707": "ML 1 Year Treasury",
    "709": "Morningstar LSTA Lvrgd Ln",
    "710": "BLOOMBERG AGGREGATE INDEX",
    "713": "BB US MBS",
    "3704": "BLOOMBERG AGGREGATE INDEX",
    "6739": "RUSSELL 1000 VALUE",
    "6741": "RUSSELL 1000 VALUE",
    "6748": "RUSSELL 1000 GROWTH",
    "3735": "BLOOMBERG AGGREGATE INDEX",
    "6781": "RUSSELL 1000 GROWTH",
    "16706": "MSCI WORLD",
    "701": "MERRILL 1-3 YEAR UST INDX",
    "711": "BB INTERMEDIATE CREDIT",
    "711": "BB US CREDIT CORP",
    "6751": "RUSSELL MIDCAP VALUE",
    "6757": "S&P GLOBAL REIT",
    "6784": "RUSSELL 1000",
    "16719": "RUSSELL 1000 VALUE",
    "3730": "JPM EMBI GLOBAL DIVERSIFI",
    "714": "BB HIGH YIELD 2% CAP",
    "16703": "RUSSELL 3000 GROWTH INDEX",
    "3237": "BB Global Agg 1-10 Yr H50",
    "3659": "BLOOMBER LONG GOVT/CREDIT",
    "3660": "BLOOMBERG AGGREGATE INDEX",
    "NETZ US Equity": "RUSSELL 1000",
    "6283": "S&P 500 INDEX",
}

portfolio_type = {
    "3750": "em",
    "6739": "equity",
    "3720": "fixed_income",
    "3738": "fixed_income",
    "3775": "fixed_income",
    "13727": "fixed_income",
    "13751": "fixed_income",
    "16705": "equity",
    "6717": "equity",
    "708": "fixed_income",
    "702": "fixed_income",
    "703": "equity",
    "704": "fixed_income",
    "705": "fixed_income",
    "706": "fixed_income",
    "707": "fixed_income",
    "709": "fixed_income",
    "710": "fixed_income",
    "713": "fixed_income",
    "3704": "fixed_income",
    "6739": "equity",
    "6741": "equity",
    "6748": "equity",
    "3735": "fixed_income",
    "6781": "equity",
    "16706": "equity",
    "701": "fixed_income",
    "711": "fixed_income",
    "712": "fixed_income",
    "6751": "equity",
    "6757": "equity",
    "6784": "equity",
    "16719": "equity",
    "3730": "equity",
    "714": "fixed_income",
    "16703": "equity",
    "3237": "fixed_income",
    "3659": "fixed_income",
    "3660": "fixed_income",
    "NETZ US Equity": "equity",
    "6283": "equity",
}


sclass_4_mapping = {
    "RENEWENERGY": "Renewable Energy, Storage, and Green Hydrogen",
    "MOBILITY": "Sustainable Mobility",
    "CIRCULARITY": "Circular Economy",
    "CCADAPT": "Climate Change Adaptation and Risk Management",
    "BIODIVERSITY": "Biodiversity & Sustainable Land & Water Use",
    "SMARTCITIES": "Sustainable Real Assets & Smart Cities",
    "EDU": "Education",
    "HEALTH": "Health",
    "SANITATION": "Sanitation & Hygiene",
    "INCLUSION": "Financial & Digital Inclusion",
    "NUTRITION": "Nutrition",
    "AFFORDABLE": "Affordable & Inclusive Housing",
    "LOWCARBON": "Low-Carbon Energy, Power, and Non-Green Hydrogen",
    "PIVOTTRANSPORT": "Pivoting Transportation",
    "MATERIALS": "Materials in Transition",
    "CARBONACCOUNT": "Carbon Accounting, Removal, & Green Finance",
    "AGRIFORESTRY": "Improving Agriculture & Forestry",
    "REALASSETS": "Transitioning Real Assets & Infrastructure",
}

risk_score_overall_mapping = {
    "Not Scored": "Not Scored",
    "Average ESG Score": "Average",
    "Leading ESG Score": "Leading",
    "Poor Risk Score": "Poor",
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

# Time Series Frequencies
frequencies = ("DAY", "WEEK", "MONTH", "QUARTER", "YEAR")
annualize_factor_d = {"WEEK": 52, "MONTH": 12, "QUARTER": 4, "YEAR": 1}
rebalance_window_d = {"MONTH": 1, "QUARTER": 3, "YEAR": 12}
