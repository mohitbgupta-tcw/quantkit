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
from bt.return_calc import SimpleReturn
from tests.shared_test_utils import *


lookback = lookback=pd.DateOffset(months=1)


@pytest.fixture
def simple_return_algo():
    return SimpleReturn(lookback)


@pytest.fixture
def simple_return_mvo_strategy(universe: pd.DataFrame, simple_return_algo: algo.Algo) -> bt.Strategy:
    '''
    Create a basic mean-var backtesting strategy that uses the simple return algo.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    simple_return_algo: algo.Algo
        instance of SimpleReturn

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
    
    return create_strategy(universe, mean_var_opt, simple_return_algo)


@pytest.fixture
def simple_return_rp_strategy(universe: pd.DataFrame, simple_return_algo: algo.Algo) -> bt.Strategy:
    '''
    Create a basic risk-parity backtesting strategy that uses the simple return algo.

    Parameters
    ----------
    names: pd.DataFrame
        tickers and prices fixture
    simple_return_algo: algo.Algo
        instance of SimpleReturn

    Returns
    -------
        target: bt.Strategy
            a basic backtesting strategy
    '''
    opt_algo = weighting_schemes.RiskParityWeight(
        lookback=pd.DateOffset(months=1))
    
    return create_strategy(universe, opt_algo, simple_return_algo)


@pytest.fixture
def simple_return_iv_strategy(universe: pd.DataFrame, simple_return_algo: algo.Algo) -> bt.Strategy:
    '''
    Create a basic inverse volatility backtesting strategy that uses the simple return algo.

    Parameters
    ----------
    names: pd.DataFrame
        tickers and prices fixture
    simple_return_algo: algo.Algo
        instance of SimpleReturn

    Returns
    -------
        target: bt.Strategy
            a basic backtesting strategy
    '''
    opt_algo = weighting_schemes.InvVolWeight(
        lookback=pd.DateOffset(months=1))
    
    return create_strategy(universe, opt_algo, simple_return_algo)


@pytest.mark.filterwarnings("ignore")
def test_simple_return_calc_mvo_strategy(universe, default_mvo_strategy, simple_return_mvo_strategy) -> None:
    """
    Run a simple mean-var strategy to exercise the simple returns calculation.
    Compare the results with a strategy where the the return calc algo is 
    not specified and confirm they are the same. They should be the same
    because the optimizer shold default to using simple returns.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    default_mvo_strategy: bt.Strategy
        weighting strategy fixture with no returns algo specified
    simple_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    """

    # Test run and assert no errors with the simple returns algo specified.
    backtest_simple_specified = bt.Backtest(simple_return_mvo_strategy, universe, integer_positions=False)
    res_simple_specified = bt.run(backtest_simple_specified)

    assert res_simple_specified
    
    df_weights_simple_specified = res_simple_specified.get_weights()

    # check the weights
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>TSLA'], 4) == 0.4333
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>TSLA'], 4) == 0.4762

    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>AAPL'], 4) == 0.5667
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>AAPL'], 4) == 0.5238

    # Run a strategy where a returns algo is not specified.
    backtest_default = bt.Backtest(default_mvo_strategy, universe, integer_positions=False)
    res_default = bt.run(backtest_default)

    assert res_default

    df_weights_default = res_default.get_weights()

    # the results weights for both strategies should be identical
    assert df_weights_simple_specified.equals(df_weights_default)


@pytest.mark.filterwarnings("ignore")
def test_simple_return_calc_rp_strategy(universe, default_rp_strategy, simple_return_rp_strategy) -> None:
    """
    Run a simple risk parity strategy to exercise the simple returns calculation.
    Compare the results with a strategy where the the return calc algo is 
    not specified and confirm they are the same. They should be the same
    because the optimizer shold default to using simple returns.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    default_mvo_strategy: bt.Strategy
        weighting strategy fixture with no returns algo specified
    simple_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    """

    # Test run and assert no errors with the simple returns algo specified.
    backtest_simple_specified = bt.Backtest(simple_return_rp_strategy, universe, integer_positions=False)
    res_simple_specified = bt.run(backtest_simple_specified)

    assert res_simple_specified
    
    df_weights_simple_specified = res_simple_specified.get_weights()

    # check the weights
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>TSLA'], 4) == 0.7081
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>TSLA'], 4) == 0.7426

    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>AAPL'], 4) == 0.2919
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>AAPL'], 4) == 0.2574

    # Run a strategy where a returns algo is not specified.
    backtest_default = bt.Backtest(default_rp_strategy, universe, integer_positions=False)
    res_default = bt.run(backtest_default)

    assert res_default

    df_weights_default = res_default.get_weights()

    # the results weights for both strategies should be identical
    assert df_weights_simple_specified.equals(df_weights_default)


@pytest.mark.filterwarnings("ignore")
def test_simple_return_calc_iv_strategy(universe, default_iv_strategy, simple_return_iv_strategy) -> None:
    """
    Run a simple inverse volatility strategy to exercise the simple returns calculation.
    Compare the results with a strategy where the the return calc algo is 
    not specified and confirm they are the same. They should be the same
    because the optimizer shold default to using simple returns.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    default_mvo_strategy: bt.Strategy
        weighting strategy fixture with no returns algo specified
    simple_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    """

    # Test run and assert no errors with the simple returns algo specified.
    backtest_simple_specified = bt.Backtest(simple_return_iv_strategy, universe, integer_positions=False)
    res_simple_specified = bt.run(backtest_simple_specified)

    assert res_simple_specified
    
    df_weights_simple_specified = res_simple_specified.get_weights()

    # check the weights
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>TSLA'], 4) == 0.7081
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>TSLA'], 4) == 0.7426

    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>AAPL'], 4) == 0.2919
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>AAPL'], 4) == 0.2574

    # Run a strategy where a returns algo is not specified.
    backtest_default = bt.Backtest(default_iv_strategy, universe, integer_positions=False)
    res_default = bt.run(backtest_default)

    assert res_default

    df_weights_default = res_default.get_weights()

    # the results weights for both strategies should be identical
    assert df_weights_simple_specified.equals(df_weights_default)


@pytest.mark.filterwarnings("ignore")
def test_simple_return_values(universe, simple_return_mvo_strategy) -> None:
    '''
    Call the algo directly to check the calculation.

    Parameters
    ----------
    universe: pd.DataFrame
        tickers and prices fixture
    simple_return_mvo_strategy:
        weighting strategy fixture with returns algo specified
    '''
    simple_return_mvo_strategy.setup(universe)
    last_date = dates[-1]
    simple_return_mvo_strategy.now = pd.to_datetime(last_date)
    simple_return_algo = SimpleReturn()

    res = simple_return_algo(simple_return_mvo_strategy)
    assert res
    assert 'return' in simple_return_mvo_strategy.temp
    
    df_return = simple_return_mvo_strategy.temp['return']
    assert len(df_return) == len(dates)-1

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'TSLA'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'TSLA'], 4) == 0.1951
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.102
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'TSLA'], 4) == 1.8148

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'AAPL'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'AAPL'], 4) == 0.619
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'AAPL'], 4) == 0.4412
    assert round(df_return.loc[pd.to_datetime('2018-01-03'), 'AAPL'], 4) == 1.3673