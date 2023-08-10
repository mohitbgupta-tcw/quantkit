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
