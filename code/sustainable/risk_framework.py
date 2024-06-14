import pandas as pd
import numpy as np
import warnings
# add quantkit to path
import sys
from pathlib import Path

d = Path().resolve().parent
sys.path.insert(0, str(d)+'/wd')
print (str(d)+'/wd')

#import quantkit.backtester.runner_backtester as runner
import quantkit.handyman.risk_framework as risk_framework

local_configs = str(Path(__file__).resolve().parent.parent.parent)+'/configs/configSA.json' 

df_detailed = risk_framework.risk_framework(local_configs=local_configs)

#print(local_configs)

import quantkit.utils.snowflake_utils as snowflake_utils

local_configs = "C:\\Users\\bastit\\OneDrive - The TCW Group Inc\\Documents\\quantkit\\configs\\configs.json"

df_detailed['Portfolio ISIN'] = df_detailed['Portfolio ISIN'].astype(str)
df_detailed['Ticker'] = df_detailed['Ticker'].astype(str)
df_detailed['As Of Date'] = df_detailed['As Of Date'].astype(str)
df_detailed['Portfolio Weight'] = df_detailed['Portfolio Weight'].astype(float)
df_detailed['Alcohol'] = df_detailed['Alcohol'].astype(float)
df_detailed['Tobacco'] = df_detailed['Tobacco'].astype(float)
df_detailed['Oil_Gas'] = df_detailed['Oil_Gas'].astype(float)
df_detailed['Gambling'] = df_detailed['Gambling'].astype(float)
df_detailed['Weapons_Firearms'] = df_detailed['Weapons_Firearms'].astype(float)
df_detailed['Thermal_Coal_Mining'] = df_detailed['Thermal_Coal_Mining'].astype(float)
df_detailed['Thermal_Coal_Power_Gen'] = df_detailed['Thermal_Coal_Power_Gen'].astype(float)
df_detailed['Adult_Entertainment'] = df_detailed['Adult_Entertainment'].astype(float)
df_detailed['Controversial_Weapons'] = df_detailed['Controversial_Weapons'].astype(str)
df_detailed['UN_Alignement'] = df_detailed['UN_Alignement'].astype(str)

snowflake_utils.write_to_snowflake(
df_detailed,
database="SANDBOX_ESG",
schema="ESG_SCORES_THEMES",
table_name="Sustainability_Framework_Detailed",
local_configs=local_configs
)

print ('---------Success---------')
