import pandas as pd
import numpy as np
import warnings
# add quantkit to path
import sys
from pathlib import Path

d = Path().resolve().parent
sys.path.insert(0, str(d)+'/wd')
#print (str(d)+'/wd')

#import quantkit.backtester.runner_backtester as runner
import quantkit.handyman.risk_framework as risk_framework

local_configs = str(Path(__file__).resolve().parent.parent.parent)+'/configs/configs.json' 

#df_detailed = risk_framework.risk_framework(local_configs=local_configs)
print(local_configs)
