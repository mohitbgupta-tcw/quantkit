import quantkit.finance.securities.securities as securitites
import quantkit.finance.data_sources.portfolio_datasource.portfolio_datasource as portfolio_datasource
import quantkit.utils.snowflake_utils as snowflake_utils
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
import datetime


class Universe(portfolio_datasource.PortfolioDataSource):
    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.tickers = dict()
        self.current_loc = 0

    def load(self, **kwargs) -> None:
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
        self.datasource.df["As Of Date"] = pd.to_datetime(
            self.datasource.df["As Of Date"]
        )
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
            ~self.datasource.df["Ticker Cd"].isin(self.params["currencies"])
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
        if not security_isin in self.securities:
            security_store = securitites.SecurityStore(
                isin=security_isin, information=security_information
            )
            ticker = security_information["Ticker Cd"]
            self.securities[security_isin] = security_store
            self.tickers[ticker] = security_store

    def iter(self) -> None:
        """
        - portfolio iter
        - Create universe df (holdings for each day)
        - Create universe dates
        """
        super().iter()
        df = self.datasource.df[["As Of Date", "Ticker Cd"]]
        self.universe_df = (
            pd.get_dummies(df, columns=["Ticker Cd"], prefix="", prefix_sep="")
            .groupby(["As Of Date"])
            .max()[self.all_tickers]
        )
        if self.params["custom_universe"]:
            self.universe_df.loc[:, :] = True
        self.universe_matrix = self.universe_df.to_numpy()

        # fundamental dates -> date + 3 months
        self.universe_dates = list(self.universe_df.index.unique())

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
            self.current_loc < len(self.universe_dates) - 1
            and date >= self.universe_dates[self.current_loc + 1]
        ):
            self.current_loc += 1
        return self.universe_matrix[self.current_loc]
