import pandas as pd
import quantkit.utils.configs as configs
import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource


def historical_portfolio_holdings(
    start_date: str,
    end_date: str,
    tcw_universe: list = [],
    equity_universe: list = [],
    fixed_income_universe: list = [],
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
    tcw_universe: list, optional
        list of tcw portfolios to run holdings for
        if not specified run default portfolios
    equity_universe: list, optional
        list of equity benchmarks to run holdings for
        if not specified run default benchmarks
    fixed_income_universe: list, optional
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
        pfs=tcw_universe,
        equity_universe=equity_universe,
        fixed_income_universe=fixed_income_universe,
    )
    return pd.df
