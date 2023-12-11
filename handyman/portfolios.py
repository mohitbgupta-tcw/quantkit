import pandas as pd
import quantkit.utils.configs as configs
import quantkit.finance.data_sources.portfolio_datasource as portfolio_datasource


def historical_portfolio_holdings(
    start_date: str,
    end_date: str,
    portfolios: list = [],
    equity_benchmark: list = [],
    fixed_income_benchmark: list = [],
    local_configs: str = "",
) -> pd.DataFrame:
    """
    Create Portfolio Holdings DataFrame

    Parameters
    ----------
    start_date: str
        start date in string format mm/dd/yyyy
    end_date: str
        end date in string format mm/dd/yyyy
    portfolios: list, optional
        list of portfolios to run holdings for
        if not specified run default portfolios
    equity_benchmark: list, optional
        list of equity benchmarks to run holdings for
        if not specified run default benchmarks
    fixed_income_benchmark: list, optional
        list of fixed income benchmarks to run holdings for
        if not specified run default benchmarks
    local_configs: str, optional
        path to a local configarations file


    Returns
    -------
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
