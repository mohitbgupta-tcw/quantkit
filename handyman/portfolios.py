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
    pf = ", ".join(f"'{pf}'" for pf in tcw_universe) if tcw_universe else "''"
    fi_benchmark = (
        ", ".join(f"'{b}'" for b in fixed_income_universe)
        if fixed_income_universe
        else "''"
    )
    e_benchmark = (
        ", ".join(f"'{b}'" for b in equity_universe)
        if equity_universe
        else "''"
    )

    and_clause = (
        f"""AND pos.portfolio_number in ({pf})"""
        if not "all" in tcw_universe
        else ""
    )
    query = f"""
    SELECT *
    FROM (
        SELECT 
            pos.as_of_date as "As Of Date",
            pos.portfolio_number AS "Portfolio",
            pos.portfolio_name AS "Portfolio Name",
            pos.isin AS "ISIN",
            sec.security_name AS "Security_Name",
            IFNULL(adj.ticker, sec.ticker) AS "Ticker Cd",
            CASE 
                WHEN rs.rclass1_name IS null 
                THEN 'Cash and Other' 
                ELSE  rs.rclass1_name 
            END AS "Sector Level 1",
            CASE 
                WHEN rs.rclass2_name IS null 
                THEN 'Cash and Other' 
                ELSE  rs.rclass2_name 
            END AS "Sector Level 2",
            sec.bclass_level4_name AS "BCLASS_Level4",
            sec.issuer_id_msci AS "MSCI ISSUERID",
            sec.issuer_id_iss AS "ISS ISSUERID",
            sec.issuer_id_bbg AS "BBG ISSUERID",
            pos.market_value_percent * 100 AS "Portfolio_Weight",
            pos.base_market_value AS "Base Mkt Val",
            null AS "OAS",
            (
                SELECT MAX(ISIN) 
                FROM tcw_core_qa.esg_iss.dim_issuer_iss iss 
                WHERE iss.issuer_id = sec.issuer_id_iss
            ) AS "Issuer ISIN"
        FROM tcw_core_qa.tcw.position_vw pos
        LEFT JOIN tcw_core_qa.tcw.security_vw sec 
            ON pos.security_key = sec.security_key
            AND pos.as_of_date = sec.as_of_date
        LEFT JOIN tcw_core_qa.reference.rclass_mapped_sectors_vw rs 
            ON sec.sclass_key  = rs.sclass_sector_key
            AND rs.rclass_scheme_name = '7. ESG - Primary Summary'
        JOIN tcw_core_qa.tcw.portfolio_vw strat 
            ON pos.portfolio_key = strat.portfolio_key 
            AND pos.as_of_date = strat.as_of_date 
            AND strat.is_active = 1
            AND strat.portfolio_type_1 IN ('Trading', 'Reporting')
        LEFT JOIN sandbox_esg.quant_research.isin_ticker_mapping adj 
            ON adj.isin = pos.isin
        WHERE pos.as_of_date >= '{start_date}'
        AND pos.as_of_date <= '{end_date}'
        {and_clause}
        UNION ALL
        --Benchmark Holdings
        SELECT  
            bench.as_of_date AS "As Of Date",
            CASE 
                WHEN bench.benchmark_name IN (
                    'S & P 500 INDEX'
                ) 
                THEN 'S&P 500 INDEX'
                WHEN bench.benchmark_name IN (
                    'Russell 1000'
                ) 
                THEN 'RUSSELL 1000'
                ELSE bench.benchmark_name 
            END AS "Portfolio",
            CASE 
                WHEN bench.benchmark_name IN (
                    'S & P 500 INDEX'
                ) 
                THEN 'S&P 500 INDEX'
                WHEN bench.benchmark_name IN (
                    'Russell 1000'
                ) 
                THEN 'RUSSELL 1000'
                ELSE bench.benchmark_name 
            END AS "Portfolio Name",
            bench.isin AS "ISIN",
            sec.security_name AS "Security_Name",
            IFNULL(adj.ticker, sec.ticker) AS "Ticker Cd",
            CASE 
                WHEN rs.rclass1_name IS null 
                THEN 'Cash and Other' 
                ELSE  rs.rclass1_name 
            END AS "Sector Level 1",
            CASE 
                WHEN rs.rclass2_name IS null
                THEN 'Cash and Other' 
                ELSE rs.rclass2_name 
            END AS "Sector Level 2",
            sec.bclass_level4_name AS "BCLASS_Level4",
            sec.issuer_id_msci AS "MSCI ISSUERID",
            sec.issuer_id_iss AS "ISS ISSUERID",
            sec.issuer_id_bbg AS "BBG ISSUERID",
            CASE
                WHEN bench.market_value_percentage IS null 
                THEN bench.market_value / SUM(bench.market_value)
                    OVER(partition BY bench.benchmark_name) 
                ELSE bench.market_value_percentage  
            END AS "Portfolio_Weight",
            bench.market_value AS "Base Mkt Val",
            null AS "OAS",
            (
                SELECT MAX(ISIN) 
                FROM tcw_core_qa.esg_iss.dim_issuer_iss iss 
                WHERE iss.issuer_id = sec.issuer_id_iss
            ) AS "Issuer ISIN"
        FROM tcw_core_qa.benchmark.benchmark_position_vw bench
        LEFT JOIN tcw_core_qa.tcw.security_vw sec 
            ON bench.security_key = sec.security_key
            AND bench.as_of_date = sec.as_of_date
        LEFT JOIN tcw_core_qa.reference.rclass_mapped_sectors_vw rs 
            ON sec.sclass_key = rs.sclass_sector_key 
            AND rs.rclass_scheme_name = '7. ESG - Primary Summary'
        LEFT JOIN sandbox_esg.quant_research.isin_ticker_mapping adj 
            ON adj.isin = bench.isin
        WHERE bench.as_of_date >= '{start_date}'
        AND bench.as_of_date <= '{end_date}'
        AND (
            universe_type_code = 'STATS' 
            AND benchmark_name IN (
                {fi_benchmark}
            ) 
            OR benchmark_name IN (
                {e_benchmark}
            )
        )
    ) 
    ORDER BY "Portfolio" ASC, "As Of Date" ASC, "Portfolio_Weight" DESC
    """
    pd.load(
        query=query
    )
    return pd.df
