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


def create_strategy(universe: list, optimizer: algo.Algo, return_calc: algo.Algo = None) -> bt.Strategy:
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

    # algo to fire on the beginning of every month
    run_monthly_algo = frequency.RunMonthly(
        run_on_first_date=False
    )

    # all securities
    select_algo = signals.SelectAll()

    # algo to rebalance the current weights to weights set in temp dictionary
    rebal_algo = portfolio_management.Rebalance()

    # add the return calc algo, if specified
    if return_calc:
        algos=[
            run_monthly_algo,
            select_algo,
            return_calc,
            optimizer,
            rebal_algo
        ]
    else:
        algos=[
            run_monthly_algo,
            select_algo,
            optimizer,
            rebal_algo
        ]

    target = bt.Strategy(
        'test strategy',
        algos=algos,
        children=securities
    )

    return target


@pytest.fixture
def default_mvo_strategy(mocker, universe: list) -> bt.Strategy:
    '''
    Create a basic mear-var backtesting strategy without specifying a returns calc algo.
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
    opt_algo = weighting_schemes.MVOWeight(
        lookback=pd.DateOffset(months=1),
        bounds=(0.0, 1.0), 
        covar_method='ledoit-wolf', 
        options={'disp': True})
    
    return create_strategy(universe, opt_algo)



@pytest.fixture
def default_rp_strategy(mocker, universe: list) -> bt.Strategy:
    '''
    Create a basic risk parity backtesting strategy without specifying a returns calc algo.
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
    
    opt_algo = weighting_schemes.RiskParityWeight(
        lookback=pd.DateOffset(months=1))
    
    return create_strategy(universe, opt_algo)


@pytest.fixture
def default_iv_strategy(mocker, universe: list) -> bt.Strategy:
    '''
    Create a basic inverse volatility backtesting strategy without specifying a returns calc algo.
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
    
    opt_algo = weighting_schemes.InvVolWeight(
        lookback=pd.DateOffset(months=1))
    
    return create_strategy(universe, opt_algo)