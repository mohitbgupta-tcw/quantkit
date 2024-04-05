import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource
import quantkit.risk_framework.financial_infrastructure.companies as comp
import quantkit.risk_framework.financial_infrastructure.munis as munis
import quantkit.risk_framework.financial_infrastructure.securitized as securitized
import quantkit.risk_framework.financial_infrastructure.sovereigns as sovereigns
import quantkit.risk_framework.financial_infrastructure.cash as cash
import quantkit.risk_framework.financial_infrastructure.securities as securities
import pandas as pd


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
        ESG Collateral Type: str
            esg collateral type
        ISIN: str
            isin of security
        Issuer ESG: str
            is issuer ESG?
        Labeled ESG Type: str
            security esg type
        Loan Category: str
            loan category
        Security_Name: str
            name of security
        TCW ESG: str
            ESG label of TCW
        Ticker Cd: str
            ticker of issuer
        Sector Level 1: str
            sector 1 of issuer
        Sector Level 2: str
            sector 2 of issuer
        JPM Sector: str
            JP Morgan sector
        BCLASS_Level2: str
            BClass Level 2 of issuer
        BCLASS_Level3: str
            BClass Level 3 of issuer
        BCLASS_level4: str
            BClass Level 4 of issuer
        Country of Risk: str
            issuer country
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
            list of all tcw portfolios to pull from API
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
                TRIM( 
                    CASE 
                        WHEN sec.esg_collateral_type IS null 
                        THEN 'Unknown' 
                        ELSE sec.esg_collateral_type 
                    END 
                ) AS "ESG Collateral Type", 
                pos.isin AS "ISIN",
                CASE 
                    WHEN sec.issuer_esg ='NA ' 
                    OR sec.issuer_esg is null  
                    THEN 'No' 
                    ELSE sec.issuer_esg 
                END AS "Issuer ESG",
                sec.loan_category AS "Loan Category",
                CASE 
                    WHEN sec.labeled_esg_type = 'None' 
                    THEN null 
                    ELSE sec.labeled_esg_type 
                END AS "Labeled ESG Type",
                sec.security_name AS "Security_Name",
                CASE 
                    WHEN sec.tcw_esg_type = 'None' 
                    THEN null 
                    ELSE sec.tcw_esg_type 
                END AS "TCW ESG",
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
                sec.jpm_level1 AS "JPM Sector",
                sec.bclass_level2_name AS "BCLASS_Level2", 
                sec.bclass_level3_name AS "BCLASS_Level3", 
                sec.bclass_level4_name AS "BCLASS_Level4",
                CASE 
                    WHEN sec.em_country_of_risk_name is null 
                    THEN sec.country_of_risk_name 
                    ELSE sec.em_country_of_risk_name 
                END AS "Country of Risk",
                sec.issuer_id_msci AS "MSCI ISSUERID",
                sec.issuer_id_iss AS "ISS ISSUERID",
                sec.issuer_id_bbg AS "BBG ISSUERID",
                pos.market_value_percent * 100 AS "Portfolio_Weight",
                pos.base_market_value AS "Base Mkt Val",
                null AS "OAS",
                (
                    SELECT MAX(ISIN) 
                    FROM tcw_core.esg_iss.dim_issuer_iss iss 
                    WHERE iss.issuer_id = sec.issuer_id_iss
                ) AS "Issuer ISIN"
            FROM tcw_core.tcw.position_vw pos
            LEFT JOIN tcw_core.tcw.security_vw sec 
                ON pos.security_key = sec.security_key
                AND pos.as_of_date = sec.as_of_date
            LEFT JOIN tcw_core.reference.rclass_mapped_sectors_vw rs 
                ON sec.sclass_key  = rs.sclass_sector_key
                AND rs.rclass_scheme_name = '7. ESG - Primary Summary'
            JOIN tcw_core.tcw.portfolio_vw strat 
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
                TRIM(
                    CASE 
                        WHEN sec.esg_collateral_type IS null 
                        THEN 'Unknown' 
                        ELSE sec.esg_collateral_type 
                    END 
                ) AS "ESG Collateral Type", 
                bench.isin AS "ISIN",
                CASE 
                    WHEN sec.issuer_esg ='NA '
                    OR sec.issuer_esg IS null
                    THEN 'No' 
                    ELSE sec.issuer_esg 
                END AS "Issuer ESG",
                sec.loan_category AS "Loan Category",
                CASE 
                    WHEN sec.labeled_esg_type = 'None'
                    THEN null 
                    ELSE sec.labeled_esg_type 
                END AS "Labeled ESG Type",
                sec.security_name AS "Security_Name",
                CASE 
                    WHEN sec.tcw_esg_type = 'None' 
                    THEN null 
                    ELSE sec.tcw_esg_type 
                END AS "TCW ESG",
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
                sec.jpm_level1 AS "JPM Sector",
                sec.bclass_level2_name AS "BCLASS_Level2",
                sec.bclass_level3_name AS "BCLASS_Level3", 
                sec.bclass_level4_name AS "BCLASS_Level4",
                CASE 
                    WHEN sec.em_country_of_risk_name is null 
                    THEN sec.country_of_risk_name 
                    ELSE sec.em_country_of_risk_name 
                END AS "Country of Risk",
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
                    FROM tcw_core.esg_iss.dim_issuer_iss iss 
                    WHERE iss.issuer_id = sec.issuer_id_iss
                ) AS "Issuer ISIN"
            FROM tcw_core.benchmark.benchmark_position_vw bench
            LEFT JOIN tcw_core.tcw.security_vw sec 
                ON bench.security_key = sec.security_key
                AND bench.as_of_date = sec.as_of_date
            LEFT JOIN tcw_core.reference.rclass_mapped_sectors_vw rs 
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

    def transform_df(self) -> None:
        """
        - Add custom EM benchmark
        - replace NA's for BCLASS and JPM Sector
        """
        super().transform_df()
        df_em = self.datasource.df[
            self.datasource.df["Portfolio"].isin(
                ["JPM CEMBI BROAD DIVERSE", "JPM EMBI GLOBAL DIVERSIFI"]
            )
        ]
        df_em["Portfolio"] = df_em["Portfolio Name"] = "JPM EM Custom Index (50/50)"
        df_em["Portfolio_Weight"] = df_em["Portfolio_Weight"] * 0.5
        self.datasource.df = pd.concat([self.datasource.df, df_em])

        replace_nas = [
            "BCLASS_Level2",
            "BCLASS_Level3",
            "BCLASS_Level4",
            "JPM Sector",
        ]
        for col in replace_nas:
            replace_df = self.datasource.df[["ISIN", col]]
            replace_df = replace_df.dropna(subset=col)
            replace_df = replace_df.drop_duplicates(subset=["ISIN"])
            replace_df = dict(zip(replace_df["ISIN"], replace_df[col]))
            self.datasource.df[col] = self.datasource.df[col].fillna(
                self.datasource.df["ISIN"].map(replace_df)
            )
        self.datasource.df["BCLASS_Level2"] = self.datasource.df[
            "BCLASS_Level2"
        ].fillna("Unassigned BCLASS")
        self.datasource.df["BCLASS_Level3"] = self.datasource.df[
            "BCLASS_Level3"
        ].fillna("Unassigned BCLASS")
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].fillna("Unassigned BCLASS")

    def iter_holdings(self, msci_dict: dict) -> None:
        """
        Iterate over portfolio holdings
        - Create Security objects
        - create Company, Muni, Sovereign, Securitized, Cash objects
        - attach holdings, OAS to self.holdings with security object

        Parameters
        ----------
        msci_dict: dict
            dictionary of msci information
        """
        security_type_mapping = {
            "Securitized": securitized.SecuritizedStore,
            "Muni": munis.MuniStore,
            "Sovereign": sovereigns.SovereignStore,
            "Cash": cash.CashStore,
            "Corporate": comp.CompanyStore,
        }

        return super().iter_holdings(msci_dict, security_type_mapping)

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
