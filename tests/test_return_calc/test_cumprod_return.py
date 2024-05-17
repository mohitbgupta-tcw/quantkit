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
from bt.return_calc import CumProdReturn, LogReturn
from tests.shared_test_utils import *


lookback = lookback=pd.DateOffset(months=1)


@pytest.fixture
def cumprod_return_algo():
    return CumProdReturn(lookback)


@pytest.fixture
def cumprod_return_mvo_strategy(universe: pd.DataFrame, cumprod_return_algo: algo.Algo) -> bt.Strategy:
    '''
    Create a basic mean-var backtesting strategy that uses the cumprod return algo.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    cumprod_return_algo: algo.Algo
        instance of CumProdReturn 

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
    
    return create_strategy(universe, mean_var_opt, cumprod_return_algo)


@pytest.mark.filterwarnings("ignore")
def test_cumprod_return_calc_mvo_strategy(universe, cumprod_return_mvo_strategy) -> None:
    """
    Run a mean-var strategy to exercise the cumprod returns calculation.
    Just checking there are no errors or exceptions.
    Here the cumprod return is calculated from simple returns and not windowed.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    cumprod_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    """

    # Test run and assert no errors with the cumprod returns algo specified.
    backtest_cumprod_specified = bt.Backtest(cumprod_return_mvo_strategy, universe, integer_positions=False)
    res_cumprod_specified = bt.run(backtest_cumprod_specified)

    assert res_cumprod_specified
    
    df_weights_cumprod_specified = res_cumprod_specified.get_weights()

    # check the weights
    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>TSLA'], 4) == 0.4333
    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>TSLA'], 4) == 0.4762

    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>AAPL'], 4) == 0.5667
    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>AAPL'], 4) == 0.5238


@pytest.mark.filterwarnings("ignore")
def test_windowed_cumprod_return_calc_mvo_strategy(universe) -> None:
    """
    Run a mean-var strategy to exercise the cumprod returns calculation.
    Just checking there are no errors or exceptions.
    Here the cumprod return is calculated from simple returns over a 1-day window.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    cumprod_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    """
    mean_var_opt = weighting_schemes.MVOWeight(
        lookback=lookback,
        bounds=(0.0, 1.0), 
        covar_method='ledoit-wolf', 
        options={'disp': True})
    
    cumprod_return_algo = CumProdReturn(window_size=1)
    cumprod_return_mvo_strategy = create_strategy(universe, mean_var_opt, cumprod_return_algo)

    # Test run and assert no errors with the cumprod returns algo specified.
    backtest_cumprod_specified = bt.Backtest(cumprod_return_mvo_strategy, universe, integer_positions=False)
    res_cumprod_specified = bt.run(backtest_cumprod_specified)

    assert res_cumprod_specified
    
    df_weights_cumprod_specified = res_cumprod_specified.get_weights()

    # check the weights
    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>TSLA'], 4) == 0.4333
    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>TSLA'], 4) == 0.4762

    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>AAPL'], 4) == 0.5667
    assert round(df_weights_cumprod_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>AAPL'], 4) == 0.5238


@pytest.mark.filterwarnings("ignore")
def test_cumprod_return_values(universe, cumprod_return_mvo_strategy) -> None:
    '''
    Call the algo directly to check the calculation.
    Run with no window and calculate the cumulative product of simple returns.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    cumprod_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    '''
    cumprod_return_mvo_strategy.setup(universe)
    last_date = dates[-1]
    cumprod_return_mvo_strategy.now = pd.to_datetime(last_date)
    cumprod_return_algo = CumProdReturn()

    res = cumprod_return_algo(cumprod_return_mvo_strategy)
    assert res
    assert 'return' in cumprod_return_mvo_strategy.temp
    
    df_return = cumprod_return_mvo_strategy.temp['return']
    assert len(df_return) == len(dates)-1

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'TSLA'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'TSLA'], 4) == 0.1951
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.3171
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'TSLA'], 4) == 2.7073

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'AAPL'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'AAPL'], 4) == 0.619
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'AAPL'], 4) == 1.3333
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'AAPL'], 4) == 4.5238


@pytest.mark.filterwarnings("ignore")
def test_log_cumprod_return_values(universe, cumprod_return_mvo_strategy) -> None:
    '''
    Call the algo directly to check the calculation.
    Run with no window and calculate the cumulative product of log returns.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    cumprod_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    '''
    cumprod_return_mvo_strategy.setup(universe)
    last_date = dates[-1]
    cumprod_return_mvo_strategy.now = pd.to_datetime(last_date)
    cumprod_return_algo = CumProdReturn()

    # set up log returns and set on strategy.temp
    log_return_algo = LogReturn()
    log_return_algo(cumprod_return_mvo_strategy)
    assert 'return' in cumprod_return_mvo_strategy.temp

    # check our initial log returns
    df_return = cumprod_return_mvo_strategy.temp['return']
    assert len(df_return) == len(dates)-1

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'TSLA'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'TSLA'], 4) == 0.1782
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.0972
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'TSLA'], 4) == 1.0349

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'AAPL'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'AAPL'], 4) == 0.4818
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'AAPL'], 4) == 0.3655
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'AAPL'], 4) == 0.8618
    
    # now run our cumprod algo and check the retuns
    res = cumprod_return_algo(cumprod_return_mvo_strategy)
    assert res
    assert 'return' in cumprod_return_mvo_strategy.temp

    df_return = cumprod_return_mvo_strategy.temp['return']
    assert len(df_return) == len(dates)-1

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'TSLA'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'TSLA'], 4) == 0.1782
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.2927
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'TSLA'], 4) == 1.6306

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'AAPL'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'AAPL'], 4) == 0.4818
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'AAPL'], 4) == 1.0234
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'AAPL'], 4) == 2.7671


@pytest.mark.filterwarnings("ignore")
def test_windowed_cumprod_return_values(universe, cumprod_return_mvo_strategy) -> None:
    '''
    Call the algo directly to check the calculation.
    Run with a 2-day window and calculate the cumulative product of simple returns.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    cumprod_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    '''
    cumprod_return_mvo_strategy.setup(universe)
    last_date = dates[-1]
    cumprod_return_mvo_strategy.now = pd.to_datetime(last_date)

    window_size=2
    cumprod_return_algo = CumProdReturn(window_size=window_size)

    res = cumprod_return_algo(cumprod_return_mvo_strategy)
    assert res
    assert 'return' in cumprod_return_mvo_strategy.temp
    
    df_return = cumprod_return_mvo_strategy.temp['return']
    assert len(df_return) == len(dates)-window_size

    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'TSLA'], 4) == 0.1951
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.3171
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'TSLA'], 4) == 2.102

    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'AAPL'], 4) == 0.619
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'AAPL'], 4) == 1.3333
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'AAPL'], 4) == 2.4118