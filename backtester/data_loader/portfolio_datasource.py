import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource
import quantkit.backtester.financial_infrastructure.securities as securities
import quantkit.utils.snowflake_utils as snowflake_utils
import datetime
import numpy as np
import pandas as pd


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
        self.current_loc = 0

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
        if self.params["custom_universe"]:
            secs = ", ".join(f"'{sec}'" for sec in self.params["custom_universe"])
            query = f"""
            SELECT  
                DISTINCT
                bench.as_of_date AS "As Of Date",
                'Custom_Portfolio' AS "Portfolio",
                'Custom_Portfolio' AS "Portfolio Name",
                bench.isin AS "ISIN",
                MAX(sec.security_name) OVER (PARTITION BY bench.isin) AS "Security_Name",
                MAX(sec.ticker) OVER (PARTITION BY bench.isin) AS "Ticker Cd",
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
                { 1 / len(self.params["custom_universe"])} AS "Portfolio_Weight",
                1 AS "Base Mkt Val",
                null AS "OAS",
                (
                    SELECT MAX(isin) 
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
            WHERE bench.as_of_date >= '{self.params["start_date"]}'
            AND bench.as_of_date <= '{self.params["end_date"]}'
            AND bench.isin in ({secs})
            ORDER BY "Portfolio" ASC,  "As Of Date" ASC, "Portfolio_Weight"  DESC
            """
        else:
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

    def transform_df(self) -> None:
        """
        - Drop Dummy values
        - Drop non tradeable, no data securities
        - Transformations for custom universe
        - Transformation for sustainable universe
        """
        super().transform_df()

        self.datasource.df.sort_values(["As Of Date", "Ticker Cd"], ascending=True)

        self.datasource.df = self.datasource.df[
            ~self.datasource.df["Security_Name"].str.contains("DUMMY")
        ]
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["Security_Name"].str.contains("MARKET VALUE ADJUSTMENT")
        ]
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["Security_Name"].str.contains("MKT VALUE ADJUST")
        ]

        self.datasource.df = self.datasource.df.dropna(subset="Ticker Cd")
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["Ticker Cd"].str.contains("_x")
        ]

        self.datasource.df = self.datasource.df[
            ~self.datasource.df["ISIN"].isin(self.params["non_puplic"])
        ]
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["ISIN"].isin(self.params["missing_data"])
        ]
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["ISIN"].isin(self.params["foreign_company"])
        ]
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["Ticker Cd"].isin(self.params["currencies"])
        ]

        if self.params["custom_universe"]:
            if self.datasource.df.empty:
                for sec in self.params["custom_universe"]:
                    sec_dict = {
                        "As Of Date": pd.bdate_range(
                            start=self.params["start_date"], end=self.params["end_date"]
                        ),
                        "Portfolio": "Custom_Portfolio",
                        "Portfolio Name": "Custom_Portfolio",
                        "ISIN": sec,
                        "Portfolio_Weight": 1 / len(self.params["custom_universe"]),
                        "Ticker Cd": sec,
                    }
                    sec_df = pd.DataFrame(data=sec_dict)
                    self.datasource.df = pd.concat([self.datasource.df, sec_df])
            else:
                self.datasource.df = self.datasource.df.drop_duplicates(
                    subset=["ISIN", "As Of Date"]
                )

        if self.params["sustainable"]:
            indices = (
                self.params["equity_universe"]
                + self.params["fixed_income_universe"]
                + self.params["tcw_universe"]
            )
            sust_df = snowflake_utils.load_from_snowflake(
                database="SANDBOX_ESG",
                schema="ESG_SCORES_THEMES",
                table_name="Sustainability_Framework_Detailed",
                local_configs={"API_settings": self.api_settings},
            )
            sust_df = sust_df[sust_df["Portfolio ISIN"].isin(indices)][
                ["Security ISIN", "SCLASS_Level2"]
            ]
            sust_df = sust_df[
                sust_df["SCLASS_Level2"].isin(["Transition", "Sustainable Theme"])
            ]
            sust_universe = list(sust_df["Security ISIN"].unique())
            self.datasource.df = self.datasource.df[
                self.datasource.df["ISIN"].isin(sust_universe)
            ]

    def iter(self) -> None:
        """
        - portfolio iter
        - Create universe df (holdings for each day)
        - Create universe dates
        """
        super().iter()

        self.universe_df = self.datasource.df.pivot_table(
            index="As Of Date",
            columns="Ticker Cd",
            values="Portfolio_Weight",
            aggfunc="sum",
        )[self.all_tickers]

        self.universe_matrix = self.universe_df.to_numpy()

        # universe dates -> date + 3 months
        self.dates = list(self.universe_df.index.unique())

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

    def outgoing_row(self, date: datetime.date) -> np.ndarray:
        """
        Return current consitutents of index universe

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        np.array
            current constitutents of universe
        """
        while (
            self.current_loc < len(self.dates) - 1
            and date >= self.dates[self.current_loc + 1]
        ):
            self.current_loc += 1
        return self.universe_matrix[self.current_loc]

    def is_valid(self, date: datetime.date) -> bool:
        """
        check if inputs are valid

        Parameters
        ----------
        date: datetimte.date
            date

        Returns
        -------
        bool
            True if inputs are valid, false otherwise
        """
        return date >= self.dates[0] and np.nansum(
            self.universe_matrix[self.current_loc] > 0
        )
