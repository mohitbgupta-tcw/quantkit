import pytest
import pandas as pd
import bt
import bt.core_structure.algo as algo

from tests.test_return_calc.utils import (
  dates,
  universe, 
  create_strategy
)

import bt.weighting_schemes as weighting_schemes
from bt.return_calc import LogReturn
from tests.shared_test_utils import *


lookback = lookback=pd.DateOffset(months=1)


@pytest.fixture
def log_return_algo():
    return LogReturn(lookback)


@pytest.fixture
def log_return_mvo_strategy(universe: pd.DataFrame, log_return_algo: algo.Algo) -> bt.Strategy:
    '''
    Create a basic mean-var backtesting strategy that uses the log return algo.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    log_return_algo: algo.Algo
        instance of LogReturn

    Returns
    -------
        target: bt.Strategy
            a basic backtesting strategy
    '''
    mean_var_opt = weighting_schemes.MVOWeight(
        lookback=lookback,
        bounds=(0.0, 1.0), 
        covar_method='ledoit-wolf', 
        options={'disp': True})
    
    return create_strategy(universe, mean_var_opt, log_return_algo)


@pytest.mark.filterwarnings("ignore")
def test_log_return_calc_mvo_strategy(universe, log_return_mvo_strategy) -> None:
    """
    Run a mean-var strategy to exercise the log returns calculation.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    log_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    """

    # Test run and assert no errors with the log returns algo specified.
    backtest_log_specified = bt.Backtest(log_return_mvo_strategy, universe, integer_positions=False)
    res_log_specified = bt.run(backtest_log_specified)

    assert res_log_specified
    
    df_weights_log_specified = res_log_specified.get_weights()

    # check the weights
    assert round(df_weights_log_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>TSLA'], 4) == 0.4333
    assert round(df_weights_log_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>TSLA'], 4) == 0.4762

    assert round(df_weights_log_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>AAPL'], 4) == 0.5667
    assert round(df_weights_log_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>AAPL'], 4) == 0.5238


@pytest.mark.filterwarnings("ignore")
def test_log_return_values(universe, log_return_mvo_strategy) -> None:
    '''
    Call the algo directly to check the calculation.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    log_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
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