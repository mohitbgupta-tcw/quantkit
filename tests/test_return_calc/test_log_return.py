import pytest
import pandas as pd
import bt
import bt.core_structure.algo as algo

from tests.test_return_calc.utils import (
  dates,
  universe, 
  create_strategy,
  default_mvo_strategy, 
  default_rp_strategy, 
  default_iv_strategy
)

import bt.weighting_schemes as weighting_schemes
from bt.return_calc import LogReturn
from tests.shared_test_utils import *


@pytest.fixture
def log_return_algo():
    return LogReturn()


@pytest.fixture
def log_return_mvo_strategy(universe: pd.DataFrame, log_return_algo: algo.Algo) -> bt.Strategy:
    '''
    Create a basic mean-var backtesting strategy that uses the log return algo.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices

    Returns
    -------
        target: bt.Strategy
            a basic backtesting strategy
    '''
    mean_var_opt = weighting_schemes.MVOWeight(
        lookback=pd.DateOffset(months=1),
        bounds=(0.0, 1.0), 
        covar_method='ledoit-wolf', 
        options={'disp': True})
    
    return create_strategy(universe, mean_var_opt, log_return_algo)


@pytest.mark.filterwarnings("ignore")
def test_log_return_values(mocker, universe, log_return_mvo_strategy) -> None:
    '''
    Call the algo directly to check the calculation.
    '''
    log_return_mvo_strategy.setup(universe)
    last_date = dates[-1]
    log_return_mvo_strategy.now = pd.to_datetime(last_date)
    log_return_algo = LogReturn()

    res = log_return_algo(log_return_mvo_strategy)
    assert res
    assert 'return' in log_return_mvo_strategy.temp
    
    df_return = log_return_mvo_strategy.temp['return']
    assert len(df_return) == len(dates)-1

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'TSLA'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'TSLA'], 4) == 0.1782
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.0972
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'TSLA'], 4) == 1.0349

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'AAPL'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'AAPL'], 4) == 0.4818
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'AAPL'], 4) == 0.3655
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'AAPL'], 4) == 0.8618