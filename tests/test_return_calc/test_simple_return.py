import pytest
import pandas as pd
import quantkit.bt as bt
from pytest_mock import mocker
import quantkit.bt.core_structure.algo as algo
from quantkit.bt.return_calc.simple_return import SimpleReturn
import quantkit.bt.frequency as frequency
import quantkit.bt.signals as signals
import quantkit.bt.weighting_schemes as weighting_schemes
import quantkit.bt.portfolio_management as portfolio_management
from quantkit.tests.shared_test_utils import *


dates = ['2017-12-30', '2017-12-31', '2018-01-01', '2018-01-02', '2018-01-03'] 


@pytest.fixture
def universe(mocker) -> pd.DataFrame:
    '''
    Create a dummy universe. This is comprised of
    tickers and prices with dates for the index.

    Returns
    -------
    pd.DataFrame:
        the universe consiting of tickers, dates and prices
    '''
    names = ['TSLA', 'AAPL']

    prices = [
        [12.3, 4.2],
        [12.3, 4.2],
        [14.7, 6.8],
        [16.2, 9.8],
        [45.6, 23.2]
    ]

    df_prices = pd.DataFrame(prices, columns=names, index=dates)
    df_prices.index = pd.to_datetime(df_prices.index)

    return df_prices


def create_strategy(universe: list, return_calc: algo.Algo = None) -> bt.Strategy:
    '''
    Create a basic backtesting stragegy.

    Parameters
    ----------
    names: list
        tickers
    return_calc: Algo, Optional

    Returns
    -------
        target: bt.Strategy
            a basic backtesting strategy
    '''
    securities = [ bt.Security( name ) for name in universe.columns ]

    # algo to fire on the beginning of every month and to run on the first date
    run_monthly_algo = frequency.RunMonthly(
        run_on_first_date=False
    )

    # all securities
    select_algo = signals.SelectAll()

    # mean-var optimization
    mean_var_opt = weighting_schemes.MVOWeight(
        lookback=pd.DateOffset(months=1),
        bounds=(0.0, 1.0), 
        covar_method='ledoit-wolf', 
        options={'disp': True})
    
    # algo to rebalance the current weights to weights set in temp dictionary
    rebal_algo = portfolio_management.Rebalance()

    # add the return calc algo, if specified
    if return_calc:
        algos=[
            run_monthly_algo,
            select_algo,
            return_calc,
            mean_var_opt,
            rebal_algo
        ]
    else:
        algos=[
            run_monthly_algo,
            select_algo,
            mean_var_opt,
            rebal_algo
        ]

    target = bt.Strategy(
        'test strategy',
        algos=algos,
        children=securities
    )

    return target


@pytest.fixture
def default_strategy(mocker, universe: list) -> bt.Strategy:
    '''
    Create a basic backtesting strategy without specifying a returns calc algo.
    The strategy should default to using simple returns for optimization.

    Parameters
    ----------
    names: list
        tickers

    Returns
    -------
        target: bt.Strategy
            a basic backtesting strategy
    '''
    return create_strategy(universe)


@pytest.fixture
def simple_return_strategy(mocker, universe: list) -> bt.Strategy:
    '''
    Create a basic backtesting stragegy that uses the simple return algo.

    Parameters
    ----------
    names: list
        tickers

    Returns
    -------
        target: bt.Strategy
            a basic backtesting strategy
    '''
     # use simple returns in the optimizer
    simple_return_algo = SimpleReturn()
    return create_strategy(universe, simple_return_algo)


def test_simple_return_calc_strategy(mocker, universe, default_strategy, simple_return_strategy) -> None:
    """
    Run a simple strategy to exercise the simple returns calculation.
    Compare the results with a strategy where the the return calc algo is 
    not specified and confirm they are the same. They should be the same
    because the optimizer shold default to using simple returns.
    """

    # Test run and assert no errors with the simple returns algo specified.
    backtest_simple_specified = bt.Backtest(simple_return_strategy, universe, integer_positions=False)
    res_simple_specified = bt.run(backtest_simple_specified)

    assert res_simple_specified
    
    df_weights_simple_specified = res_simple_specified.get_weights()

    # check the weights
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>TSLA'], 4) == 0.4333
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>TSLA'], 4) == 0.4762

    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-02'), 'test strategy>AAPL'], 4) == 0.5667
    assert round(df_weights_simple_specified.loc[pd.to_datetime('2018-01-03'), 'test strategy>AAPL'], 4) == 0.5238

    # Run a strategy where a returns algo is not specified.
    backtest_default = bt.Backtest(default_strategy, universe, integer_positions=False)
    res_default = bt.run(backtest_default)

    assert res_default

    df_weights_default = res_default.get_weights()

    # the results weights for both strategies should be identical
    assert df_weights_simple_specified.equals(df_weights_default)


def test_simple_return_values(mocker, universe, simple_return_strategy) -> None:
    '''
    Call the algo directly to check the calculation.
    '''
    simple_return_strategy.setup(universe)
    last_date = dates[-1]
    simple_return_strategy.now = pd.to_datetime(last_date)
    simple_return_algo = SimpleReturn()

    res = simple_return_algo(simple_return_strategy)
    assert res
    assert 'return' in simple_return_strategy.temp
    
    df_return = simple_return_strategy.temp['return']
    assert len(df_return) == len(dates)-1

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'TSLA'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.102
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'TSLA'], 4) == 0.102

    assert round(df_return.loc[pd.to_datetime('2017-12-31'), 'AAPL'], 4) == 0.
    assert round(df_return.loc[pd.to_datetime('2018-01-01'), 'AAPL'], 4) == 0.619
    assert round(df_return.loc[pd.to_datetime('2018-01-02'), 'AAPL'], 4) == 0.4412