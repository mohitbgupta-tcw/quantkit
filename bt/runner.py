from quantkit.bt.backtest_engine.results import Result, RandomBenchmarkResult
from quantkit.bt.backtest_engine.backtest import Backtest


def run(*backtests):
    """
    Run series of backtests

    Parameters
    ----------
    backtests: *list
        list of backtests to run

    Returns
    -------
    Result:
        result of backtests
    """
    # run each backtest
    for bkt in backtests:
        bkt.run()

    return Result(*backtests)


def benchmark_random(backtest, random_strategy, nsim: int = 100):
    """
    Given a backtest and a random strategy, compare backtest to a number of random portfolios.
    Random strategy should have at least one of the following:
        - RandomWeight
        - SelectRandomly

    Parameters
    ----------
    backtest: Backtest
        backtest of interest
    random_strategy: Strategy
        strategy with random component to benchmark against
    nsim: int
        number of random strategies to create

    Returns
    -------
    RandomBenchmarkResult:
        result of default and random backtests
    """
    if backtest.name is None:
        backtest.name = "original"

    # run if necessary
    if not backtest.has_run:
        backtest.run()

    bts = []
    bts.append(backtest)
    data = backtest.data.dropna()

    # create and run random backtests
    for i in range(nsim):
        random_strategy.name = "random_%s" % i
        rbt = Backtest(random_strategy, data)
        rbt.run()

        bts.append(rbt)
    return RandomBenchmarkResult(*bts)
