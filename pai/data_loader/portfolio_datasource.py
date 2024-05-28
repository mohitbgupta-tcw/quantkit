import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource
import quantkit.pai.financial_infrastructure.securities as securities
import quantkit.pai.financial_infrastructure.portfolios as portfolios
import os


quant_core_db = os.environ.get('QUANT_CORE_DB', 'tcw_core_dev')

class PortfolioDataSource(portfolio_datasource.PortfolioDataSource):
    """
    Provide portfolio data

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        As Of Date: datetime.date
            date of portfolio
        Portfolio: str
            portfolio id
        Portfolio name: str
            portfolio name
        ISIN: str
            isin of security
        Security_Name: str
            name of security
        Ticker Cd: str
            ticker of issuer
        Sector Level 1: str
            sector 1 of issuer
        Sector Level 2: str
            sector 2 of issuer
        BCLASS_level4: str
            BClass Level 4 of issuer
        MSCI ISSUERID: str
            msci issuer id
        ISS ISSUERID: str
            iss issuer id
        BBG ISSUERID: str
            bloomberg issuer id
        Issuer ISIN: str
            issuer isin
        Portfolio_Weight: float
            weight of security in portfolio
        Base Mkt Value: float
            market value of position in portfolio
        OAS: float
            OAS
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)

    def load(
        self,
        start_date: str,
        end_date: str,
        equity_universe: list,
        fixed_income_universe: list,
        tcw_universe: list = [],
    ) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        start_date: str
            start date to pull from API
        end_date: str
            end date to pull from API
        equity_universe: list
            list of all equity benchmarks to pull from API
        fixed_income_universe: list
            list of all equity benchmarks to pull from API
        tcw_universe: list, optional
            list of all TCW portfolios to pull from API
        """

        pf = ", ".join(f"'{pf}'" for pf in tcw_universe) if tcw_universe else "''"
        fi_benchmark = (
            ", ".join(f"'{b}'" for b in fixed_income_universe)
            if fixed_income_universe
            else "''"
        )
        e_benchmark = (
            ", ".join(f"'{b}'" for b in equity_universe) if equity_universe else "''"
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
                CASE 
                    WHEN sec.labeled_esg_type = 'None' 
                    THEN null 
                    ELSE sec.labeled_esg_type 
                END AS "Labeled ESG Type",
                sec.bclass_level4_name AS "BCLASS_Level4",
                sec.issuer_id_msci AS "MSCI ISSUERID",
                sec.issuer_id_iss AS "ISS ISSUERID",
                sec.issuer_id_bbg AS "BBG ISSUERID",
                pos.market_value_percent * 100 AS "Portfolio_Weight",
                pos.base_market_value AS "Base Mkt Val",
                null AS "OAS",
                (
                    SELECT MAX(ISIN) 
                    FROM {quant_core_db}.esg_iss.dim_issuer_iss iss 
                    WHERE iss.issuer_id = sec.issuer_id_iss
                ) AS "Issuer ISIN"
            FROM {quant_core_db}.tcw.position_vw pos
            LEFT JOIN {quant_core_db}.tcw.security_vw sec 
                ON pos.security_key = sec.security_key
                AND pos.as_of_date = sec.as_of_date
            LEFT JOIN {quant_core_db}.reference.rclass_mapped_sectors_vw rs 
                ON sec.sclass_key  = rs.sclass_sector_key
                AND rs.rclass_scheme_name = '7. ESG - Primary Summary'
            JOIN {quant_core_db}.tcw.portfolio_vw strat 
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
                CASE 
                    WHEN sec.labeled_esg_type = 'None'
                    THEN null 
                    ELSE sec.labeled_esg_type 
                END AS "Labeled ESG Type",
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
                    FROM {quant_core_db}.esg_iss.dim_issuer_iss iss 
                    WHERE iss.issuer_id = sec.issuer_id_iss
                ) AS "Issuer ISIN"
            FROM {quant_core_db}.benchmark.benchmark_position_vw bench
            LEFT JOIN {quant_core_db}.tcw.security_vw sec 
                ON bench.security_key = sec.security_key
                AND bench.as_of_date = sec.as_of_date
            LEFT JOIN {quant_core_db}.reference.rclass_mapped_sectors_vw rs 
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
        super().load(query=query)

    def create_security_store(
        self, security_isin: str, security_information: dict
    ) -> None:
        """
        create security store

        Parameters
        ----------
        security isin: str
            isin of security
        security_information: dict
            dictionary of information about the security
        """
        security_store = securities.SecurityStore(
            isin=security_isin, information=security_information
        )
        self.securities[security_isin] = security_store

        ticker = security_information["Ticker Cd"]
        self.tickers[ticker] = security_store

    def iter(self) -> None:
        """
        - iterate over portfolios and:
            - Create Portfolio Objects
            - Save in self.portfolios
            - key is portfolio id
            - add holdings df
        - save all held securities
        """
        self.all_tickers = list(self.df["Ticker Cd"].unique())
        for index, row in (
            self.df[["Portfolio", "Portfolio Name"]].drop_duplicates().iterrows()
        ):
            pf = row["Portfolio"]
            pf_store = portfolios.PortfolioStore(pf=pf, name=row["Portfolio Name"])
            holdings_df = self.df[self.df["Portfolio"] == pf][
                ["As Of Date", "ISIN", "Portfolio_Weight", "Base Mkt Val", "OAS"]
            ]
            pf_store.add_holdings(holdings_df)
            as_of_date = (
                self.df[self.df["Portfolio"] == pf]
                .groupby("Portfolio")["As Of Date"]
                .max()
                .values[0]
            )
            pf_store.add_as_of_date(as_of_date)
            self.portfolios[pf] = pf_store
