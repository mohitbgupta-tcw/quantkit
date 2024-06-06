import pandas as pd
import numpy as np
import warnings
# add quantkit to path
import sys
from pathlib import Path

d = Path().resolve().parent
sys.path.insert(0, str(d)+'/wd')
#print (str(d)+'/wd')

import quantkit.backtester.runner_backtester as runner

warnings.filterwarnings("ignore")
local_configs = "configs_q_alpha.json"
r = runner.Runner()
r.init(local_configs=local_configs)



