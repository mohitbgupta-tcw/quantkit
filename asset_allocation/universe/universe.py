import quantkit.finance.data_sources.portfolio_datasource.portfolio_datasource as portfolio_datasource
import quantkit.utils.snowflake_utils as snowflake_utils
import quantkit.utils.logging as logging


class Universe(portfolio_datasource.PortfolioDataSource):
    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)

    def load(
        self,
    ) -> None:
        if self.params["custom_universe"]:
            logging.log("Loading Portfolio Data")
            secs = ", ".join(f"'{sec}'" for sec in self.params["custom_universe"])
            query = f"""
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
                sec.id_ticker as "Ticker Cd",
                CASE 
                    WHEN rs.report_sector1_name IS null 
                    THEN 'Cash and Other' 
                    ELSE  rs.report_sector1_name 
                END AS "Sector Level 1",
                CASE 
                    WHEN rs.report_sector2_name IS null
                    THEN 'Cash and Other' 
                    ELSE rs.report_sector2_name 
                END AS "Sector Level 2",
                sec.jpm_sector_level1 AS "JPM Sector",
                sec.bclass_level2 AS "BCLASS_Level2",
                sec.bclass_level3 AS "BCLASS_Level3", 
                sec.bclass_level4 AS "BCLASS_Level4",
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
                    ELSE bench.MARKET_VALUE_PERCENTAGE 
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
            LEFT JOIN tcw_core_qa.reference.report_sectors_map_vw rs 
                ON bench.core_sector_key = rs.sector_key 
                AND rs.report_scheme = '7. ESG - Primary Summary'
            WHERE bench.as_of_date >= '{self.params["start_date"]}'
            AND bench.as_of_date <= '{self.params["end_date"]}'
            AND ISIN in ({secs})
        ORDER BY "Portfolio" ASC,  "As Of Date" ASC, "Portfolio_Weight"  DESC
        """
            self.datasource.load(query=query)
            self.transform_df()
        else:
            super().load(
                start_date=self.params["start_date"],
                end_date=self.params["end_date"],
                equity_benchmark=self.params["equity_universe"],
                fixed_income_benchmark=self.params["fixed_income_universe"],
                pfs=self.params["tcw_universe"],
            )

    def transform_df(self) -> None:
        """
        Transformations for custom universe and sustainable universe
        """
        super().transform_df()
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["Security_Name"].str.contains("DUMMY")
        ]
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["Ticker Cd"].str.contains("_x")
        ]
        self.datasource.df = self.datasource.df[
            ~self.datasource.df["ISIN"].isin(self.params["non_puplic"])
        ]

        if self.params["custom_universe"]:
            self.datasource.df = self.datasource.df.drop_duplicates(
                subset=["ISIN", "As Of Date"]
            )
            self.datasource.df["Portfolio"] = "Test_Portfolio"
            self.datasource.df["Portfolio Name"] = "Test_Portfolio"
            self.datasource.df["Portfolio_Weight"] = 1 / len(
                self.params["custom_universe"]
            )
            self.datasource.df["Ticker Cd"] = self.datasource.df["Ticker Cd"].replace(
                to_replace="/", value=".", regex=True
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
