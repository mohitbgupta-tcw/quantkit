import pandas as pd
import numpy as np
import warnings
# add quantkit to path
import sys
from pathlib import Path

d = Path().resolve().parent
sys.path.insert(0, str(d)+'/wd')

print (str(d)+'/wd')
print ('-------------------1111111111111111111------------------------')
print (Path().resolve().parent)
print ('-------------------22------------------------')
print (sys.path)
print ('-------------------33------------------------')
import quantkit.backtester.runner_backtester as runner



