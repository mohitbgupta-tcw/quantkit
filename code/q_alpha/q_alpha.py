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
local_configs = str(Path(__file__).resolve().parent.parent)+'/q_alpha/configs_q_alpha.json'
#print(local_configs)
r = runner.Runner()
r.init(local_configs=local_configs)

r.run()
print ("hello-20240612")


