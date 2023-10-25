import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.finance.portfolios.portfolios as portfolios
import quantkit.finance.companies.companies as comp
import quantkit.finance.companies.munis as munis
import quantkit.finance.companies.securitized as securitized
import quantkit.finance.companies.sovereigns as sovereigns
import quantkit.finance.companies.cash as cash
import quantkit.finance.securities.securities as securitites
import quantkit.handyman.msci_data_loader as msci_data_loader
import pandas as pd
import numpy as np
from copy import deepcopy


class PortfolioDataSource(ds.DataSources):
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
        self.portfolios = dict()
        self.securities = dict()
        self.companies = dict()
        self.munis = dict()
        self.sovereigns = dict()
        self.securitized = dict()
        self.cash = dict()

    def load(
        self,
        as_of_date: str,
        equity_benchmark: list,
        fixed_income_benchmark: list,
        pfs: list = [],
    ) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        as_of_date: str,
            date to pull from API
        equity_benchmark: list
            list of all equity benchmarks to pull from API
        fixed_income_benchmark: list
            list of all equity benchmarks to pull from API
        pfs: list, optional
            list of all portfolios to pull from API
        """
        logging.log("Loading Portfolio Data")

        pf = ", ".join(f"'{pf}'" for pf in pfs) if pfs else "''"
        fi_benchmark = (
            ", ".join(f"'{b}'" for b in fixed_income_benchmark)
            if fixed_income_benchmark
            else "''"
        )
        e_benchmark = (
            ", ".join(f"'{b}'" for b in equity_benchmark) if equity_benchmark else "''"
        )

        and_clause = f"""AND pos.portfolio_number in ({pf})""" if pfs else ""

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
                sec.id_ticker AS "Ticker Cd",
                CASE 
                    WHEN rs.report_sector1_name IS null 
                    THEN 'Cash and Other' 
                    ELSE  rs.report_sector1_name 
                END AS "Sector Level 1",
                CASE 
                    WHEN rs.report_sector2_name IS null 
                    THEN 'Cash and Other' 
                    ELSE  rs.report_sector2_name 
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
                pos.pct_market_value * 100 AS "Portfolio_Weight",
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
            LEFT JOIN tcw_core_qa.reference.report_sectors_map_vw rs 
                ON sec.sector_key_tclass = rs.sector_key 
                AND rs.report_scheme = '7. ESG - Primary Summary'
            JOIN tcw_core_qa.tcw.portfolio_vw strat 
                ON pos.portfolio_key = strat.portfolio_key 
                AND pos.as_of_date = strat.as_of_date 
                AND strat.is_active = 1
            WHERE pos.as_of_date = '{as_of_date}'
            {and_clause}
            UNION
            --Benchmark Holdings
            SELECT  
                bench.as_of_date AS "As Of Date",
                CASE 
                    WHEN bench.benchmark_name IN (
                        'JPM CEMBI BROAD DIVERSE',
                        'JPM EMBI GLOBAL DIVERSIFI'
                    ) 
                    THEN 'JPM EM Custom Index (50/50)'
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
                        'JPM CEMBI BROAD DIVERSE',
                        'JPM EMBI GLOBAL DIVERSIFI'
                    ) 
                    THEN 'JPM EM Custom Index (50/50)' 
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
                (
                    CASE 
                        WHEN bench.benchmark_name IN (
                            'JPM CEMBI BROAD DIVERSE',
                            'JPM EMBI GLOBAL DIVERSIFI'
                        ) 
                        THEN (
                            CASE 
                                WHEN bench.market_value_percentage IS null 
                                THEN bench.market_value / SUM(bench.market_value) 
                                    OVER(partition BY bench.benchmark_name) 
                                ELSE  bench.market_value_percentage 
                            END
                        )*0.5 
                        ELSE (
                            CASE 
                                WHEN bench.market_value_percentage IS null 
                                THEN bench.market_value / SUM(bench.market_value)
                                    OVER(partition BY bench.benchmark_name) 
                                ELSE bench.MARKET_VALUE_PERCENTAGE 
                            END
                        )
                    END
                )*100  AS "Portfolio_Weight",
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
            WHERE bench.as_of_date = '{as_of_date}'
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
        ORDER BY "Portfolio" ASC
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - replace NA's in ISIN with Name of Cash
        - replace NA's in BCLASS_Level4 with 'Unassigned BCLASS'
        - change first letter of each word to upper, else lower case
        - replace NA's in several columns
        - replace NA's of MSCI ISSUERID by running MSCI API
        - reaplace transformation values
        """
        self.datasource.df.replace("N/A", np.nan, inplace=True)
        self.datasource.df = self.datasource.df.fillna(value=np.nan)

        self.datasource.df.loc[
            (self.datasource.df["ISIN"].isna())
            & self.datasource.df["Security_Name"].isna(),
            ["ISIN", "Security_Name"],
        ] = "Cash"
        self.datasource.df["ISIN"].fillna(
            self.datasource.df["Security_Name"], inplace=True
        )
        self.datasource.df["Security_Name"].fillna(
            self.datasource.df["ISIN"], inplace=True
        )
        self.datasource.df["Issuer ISIN"].fillna(
            self.datasource.df["ISIN"], inplace=True
        )

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

        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].str.title()
        self.datasource.df["BCLASS_Level2"] = self.datasource.df[
            "BCLASS_Level2"
        ].fillna("Unassigned BCLASS")
        self.datasource.df["BCLASS_Level3"] = self.datasource.df[
            "BCLASS_Level3"
        ].fillna("Unassigned BCLASS")
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].fillna("Unassigned BCLASS")

        sec_isins = list(
            self.datasource.df[self.datasource.df["MSCI ISSUERID"].isna()]["ISIN"]
            .dropna()
            .unique()
        )
        msci_d = msci_data_loader.create_msci_mapping("ISIN", sec_isins)[
            ["Client_ID", "ISSUERID"]
        ]
        msci_d = dict(zip(msci_d["Client_ID"], msci_d["ISSUERID"]))
        msci_d.pop("Cash", None)
        self.datasource.df["MSCI ISSUERID"] = self.datasource.df[
            "MSCI ISSUERID"
        ].fillna(self.datasource.df["ISIN"].map(msci_d))

        self.datasource.df["MSCI ISSUERID"].fillna("NoISSUERID", inplace=True)
        self.datasource.df["BBG ISSUERID"].fillna("NoISSUERID", inplace=True)
        self.datasource.df["ISS ISSUERID"].fillna("NoISSUERID", inplace=True)

        self.datasource.df["Portfolio_Weight"] = self.datasource.df[
            "Portfolio_Weight"
        ].astype(float)
        self.datasource.df["Base Mkt Val"] = self.datasource.df["Base Mkt Val"].astype(
            float
        )

        if self.params.get("transformation"):
            for sec, trans in self.params["transformation"].items():
                for col, col_value in trans.items():
                    self.datasource.df.loc[
                        self.datasource.df["ISIN"] == sec, col
                    ] = col_value

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
            self.df[["As Of Date", "Portfolio", "Portfolio Name"]]
            .drop_duplicates()
            .iterrows()
        ):
            pf = row["Portfolio"]
            pf_store = portfolios.PortfolioStore(pf=pf, name=row["Portfolio Name"])
            holdings_df = self.df[self.df["Portfolio"] == pf][
                ["As Of Date", "ISIN", "Portfolio_Weight", "Base Mkt Val", "OAS"]
            ]
            pf_store.add_holdings(holdings_df)
            pf_store.add_as_of_date(row["As Of Date"])
            self.portfolios[pf] = pf_store

    def iter_holdings(
        self,
        msci_dict: dict,
    ) -> None:
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
        logging.log("Iterate Holdings")
        for index, row in self.df.iterrows():
            sec_info = (
                row[
                    [
                        "ESG Collateral Type",
                        "ISIN",
                        "Issuer ESG",
                        "Loan Category",
                        "Labeled ESG Type",
                        "Security_Name",
                        "TCW ESG",
                        "Ticker Cd",
                        "Sector Level 1",
                        "Sector Level 2",
                        "BCLASS_Level4",
                        "MSCI ISSUERID",
                        "ISS ISSUERID",
                        "BBG ISSUERID",
                        "Issuer ISIN",
                        "BCLASS_Level2",
                        "BCLASS_Level3",
                        "JPM Sector",
                        "Country of Risk",
                    ]
                ]
                .squeeze()
                .to_dict()
            )

            pf = row["Portfolio"]  # portfolio id
            isin = sec_info["ISIN"]  # security isin

            # get MSCI information
            msci_information = self.create_msci_information(sec_info, msci_dict)

            # create security store
            self.create_security_store(isin, sec_info)
            security_store = self.securities[isin]

            # create company object
            issuer = sec_info["Issuer ISIN"]
            self.create_parent_store(
                sector_level_2=row["Sector Level 2"],
                issuer_isin=issuer,
                msci_information=msci_information,
                security_store=security_store,
            )

            # attach security object, portfolio weight, OAS to portfolio
            holding_measures = row[
                ["Portfolio_Weight", "Base Mkt Val", "OAS"]
            ].to_dict()
            self.portfolios[pf].holdings[isin] = self.portfolios[pf].holdings.get(
                isin,
                {
                    "object": security_store,
                    "holding_measures": [],
                },
            )
            self.portfolios[pf].holdings[isin]["holding_measures"].append(
                holding_measures
            )

            # attach portfolio to security
            security_store.portfolio_store[pf] = self.portfolios[pf]

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
            self.securities[security_isin] = security_store

    def create_parent_store(
        self,
        sector_level_2: str,
        issuer_isin: str,
        msci_information: dict,
        security_store: securitites.SecurityStore,
    ) -> None:
        """
        create new objects for Company, Muni, Sovereign and Securitized, Cash

        Parameters
        ----------
        sector_level_2: str
            Sector Level 2
        issuer_isin: str
            parent issuer isin
        msci_information: dict
            dictionary of msci information on company level
        security_store: SecurityStore
            security object
        """
        security_type_mapping = {
            "Securitized": securitized.SecuritizedStore,
            "Muni": munis.MuniStore,
            "Sovereign": sovereigns.SovereignStore,
            "Cash": cash.CashStore,
            "Corporate": comp.CompanyStore,
        }

        # attach information to security's company
        # create new objects for Muni, Sovereign and Securitized
        if sector_level_2 in ["Muni / Local Authority"]:
            check_type = "Muni"
            all_parents = self.munis
        elif sector_level_2 in ["Residential MBS", "CMBS", "ABS"]:
            check_type = "Securitized"
            all_parents = self.securitized
        elif sector_level_2 in ["Sovereign"]:
            check_type = "Sovereign"
            all_parents = self.sovereigns
        elif sector_level_2 in ["Cash and Other"]:
            check_type = "Cash"
            all_parents = self.cash
        else:
            check_type = "Corporate"
            all_parents = self.companies

        class_ = security_type_mapping[check_type]
        all_parents[issuer_isin] = all_parents.get(
            issuer_isin, class_(issuer_isin, msci_information)
        )

        # attach security to company and vice versa
        all_parents[issuer_isin].add_security(security_store.isin, security_store)
        security_store.parent_store = all_parents[issuer_isin]

    def create_msci_information(
        self, security_information: dict, msci_dict: dict
    ) -> dict:
        """
        Get MSCI information of company

        Parameters
        ----------
        security_information: dict
            dictionary of information about the security
        msci_dict: dict
            dictionary of all MSCI information

        Returns
        -------
        dict
            dictionary of MSCI information
        """
        issuer_id = security_information["MSCI ISSUERID"]
        issuer_id = issuer_id if issuer_id in msci_dict else "NoISSUERID"

        issuer_dict = deepcopy(msci_dict[issuer_id])
        issuer_isin = issuer_dict["ISSUER_ISIN"]
        if not pd.isna(issuer_isin):
            security_information["Issuer ISIN"] = issuer_isin
        else:
            issuer_dict["ISSUER_ISIN"] = security_information["ISIN"]
            issuer_dict["ISSUER_NAME"] = security_information["Security_Name"]
        return issuer_dict

    @property
    def all_msci_ids(self):
        return list(self.df["MSCI ISSUERID"].dropna().unique())

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
