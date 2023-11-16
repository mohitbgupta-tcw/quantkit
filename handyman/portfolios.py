import pandas as pd
import quantkit.utils.configs as configs
import quantkit.finance.data_sources.portfolio_datasource.portfolio_datasource as portfolio_datasource


def historical_portfolio_holdings(
    start_date: str,
    end_date: str,
    portfolios: list = None,
    equity_benchmark: list = None,
    fixed_income_benchmark: list = None,
    local_configs: str = "",
) -> pd.DataFrame:
    """
    Create Portfolio Holdings DataFrame

    Parameters
    ----------
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    as_of_date: str
        as of date in string format mm/dd/yyyy
    portfolios: list, optional
        list of portfolios to run holdings for
        if not specified run default portfolios
    equity_benchmark: list, optional
        list of equity benchmarks to run holdings for
        if not specified run default benchmarks
    fixed_income_benchmark: list, optional
        list of fixed income benchmarks to run holdings for
        if not specified run default benchmarks
    pd.DataFrame
        Portfolio Holdings
    """
    params = configs.read_configs(local_configs=local_configs)

    api_settings = params["API_settings"]
    pd = portfolio_datasource.PortfolioDataSource(
        params=params["portfolio_datasource"], api_settings=api_settings
    )
    pd.load(
        start_date=start_date,
        end_date=end_date,
        pfs=portfolios,
        equity_benchmark=equity_benchmark,
        fixed_income_benchmark=fixed_income_benchmark,
    )
    return pd.df
