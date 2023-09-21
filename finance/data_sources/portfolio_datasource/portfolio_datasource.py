import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.finance.portfolios.portfolios as portfolios
import quantkit.finance.companies.companies as comp
import quantkit.finance.securities.securities as secs
import quantkit.finance.sectors.sectors as sectors
import quantkit.utils.mapping_configs as mapping_configs
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
        ISSUER_NAME: str
            name of issuer
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
        self.companies = dict()
        self.munis = dict()
        self.sovereigns = dict()
        self.securitized = dict()

    def load(
        self,
        as_of_date: str,
        pfs: list,
        equity_benchmark: list,
        fixed_income_benchmark: list,
    ) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        as_of_date: str,
            date to pull from API
        pfs: list
            list of all portfolios to pull from API
        equity_benchmark: list
            list of all equity benchmarks to pull from API
        fixed_income_benchmark: list
            list of all equity benchmarks to pull from API
        """
        logging.log("Loading Portfolio Data")
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
                CASE
                    WHEN pos.isin ='N/A' 
                    THEN null
                    ELSE pos.isin
                END AS "ISIN",
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
                sec.security_name AS "ISSUER_NAME",
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
                CASE 
                    WHEN sec.bclass_level4 = 'N/A' 
                    THEN 'Unassigned BCLASS' 
                    ELSE sec.bclass_level4 
                END AS "BCLASS_Level4",
                sec.em_country_of_risk_name AS "Country of Risk",
                sec.issuer_id_msci AS "MSCI ISSUERID",
                sec.issuer_id_iss AS "ISS ISSUERID",
                sec.issuer_id_bbg AS "BBG ISSUERID",
                pos.pct_market_value * 100 AS "Portfolio_Weight",
                pos.base_market_value AS "Base Mkt Val",
                null AS "OAS",
                (
                    SELECT MAX(ISIN) 
                    FROM tcw_core_dev.esg_iss.dim_issuer_iss iss 
                    WHERE iss.issuer_id = sec.issuer_id_iss
                ) AS "Issuer ISIN"
            FROM tcw_core_dev.tcw.position_vw pos
            LEFT JOIN tcw_core_dev.tcw.security_vw sec 
                ON pos.security_key = sec.security_key
                AND pos.as_of_date = sec.as_of_date
            LEFT JOIN tcw_core_dev.reference.report_sectors_map_vw rs 
                ON sec.sector_key_tclass = rs.sector_key 
                AND rs.report_scheme = '7. ESG - Primary Summary'
            WHERE pos.as_of_date = '{as_of_date}'
            AND pos.portfolio_number in (
                {', '.join(f"'{pf}'" for pf in pfs)}
            )
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
                CASE
                    WHEN bench.isin ='N/A' 
                    THEN null
                    ELSE bench.isin
                END AS "ISIN",
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
                sec.security_name AS "ISSUER_NAME",
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
                CASE 
                    WHEN sec.bclass_level4 = 'N/A' 
                    THEN 'Unassigned BCLASS' 
                    ELSE sec.bclass_level4 
                END AS "BCLASS_Level4",
                sec.em_country_of_risk_name AS "Country of Risk",
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
                    FROM tcw_core_dev.esg_iss.dim_issuer_iss iss 
                    WHERE iss.issuer_id = sec.issuer_id_iss
                ) AS "Issuer ISIN"
            FROM tcw_core_dev.benchmark.benchmark_position_vw bench
            LEFT JOIN tcw_core_dev.tcw.security_vw sec 
                ON bench.security_key = sec.security_key
                AND bench.as_of_date = sec.as_of_date
            LEFT JOIN tcw_core_dev.reference.report_sectors_map_vw rs 
                ON bench.core_sector_key = rs.sector_key 
                AND rs.report_scheme = '7. ESG - Primary Summary'
            WHERE bench.as_of_date = '{as_of_date}'
            AND (
                universe_type_code = 'STATS' 
                AND benchmark_name IN (
                    {', '.join(f"'{b}'" for b in fixed_income_benchmark)}
                ) 
                OR benchmark_name IN (
                    {', '.join(f"'{b}'" for b in equity_benchmark)}
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
        self.datasource.df.loc[
            (self.datasource.df["ISIN"].isna())
            & self.datasource.df["ISSUER_NAME"].isna(),
            "ISIN",
        ] = "Cash"
        self.datasource.df["ISIN"].fillna(
            self.datasource.df["ISSUER_NAME"], inplace=True
        )
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].fillna("Unassigned BCLASS")
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].str.title()
        self.datasource.df["BCLASS_Level4"].replace(
            "Unassigned Bclass", "Unassigned BCLASS", inplace=True
        )

        replace_nas = [
            "BCLASS_Level2",
            "BCLASS_Level3",
            "JPM Sector",
            "Country of Risk",
        ]
        for col in replace_nas:
            replace_df = self.datasource.df[["ISIN", col]]
            replace_df = replace_df.dropna(subset=col)
            replace_df = replace_df.drop_duplicates(subset=["ISIN"])
            self.datasource.df = pd.merge(
                left=self.datasource.df, right=replace_df, how="left", on="ISIN"
            )
            self.datasource.df[col] = self.datasource.df[f"{col}_y"]
            self.datasource.df = self.datasource.df.drop(
                [f"{col}_x", f"{col}_y"], axis=1
            )

        local_configs = "C:\\Users\\bastit\\Documents\\quantkit\\configs\\configs.json"
        sec_isins = list(
            self.datasource.df[self.datasource.df["MSCI ISSUERID"].isna()]["ISIN"]
            .dropna()
            .unique()
        )
        msci_df = msci_data_loader.create_msci_mapping(
            "ISIN", sec_isins, local_configs
        )[["Client_ID", "ISSUERID"]]
        self.datasource.df = pd.merge(
            left=self.datasource.df,
            right=msci_df,
            left_on="ISIN",
            right_on="Client_ID",
            how="left",
        )
        self.datasource.df["MSCI ISSUERID"] = self.datasource.df[
            "MSCI ISSUERID"
        ].fillna(self.datasource.df["ISSUERID"])
        self.datasource.df = self.datasource.df.drop(["Client_ID", "ISSUERID"], axis=1)

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

        # save all securities that occur in portfolios to filter down security database later on
        self.all_holdings = list(self.df["ISIN"].unique())

    def iter_holdings(
        self,
        securities: dict,
        securitized_mapping: dict,
        bclass_dict: dict,
        sec_adjustment_dict: dict,
        bloomberg_dict: dict,
        sdg_dict: dict,
    ) -> None:
        """
        Iterate over portfolio holdings
        - attach ESG information so security
        - create Muni, Sovereign, Securitized objects
        - attach sector information to company
        - attach BCLASS to company
        - attach Bloomberg information to company
        - attach holdings, OAS to self.holdings with security object

        Parameters
        ----------
        securities: dict
            dictionary of all security objects
        securitized_mapping: dict
            mapping for ESG Collat Type
        bclass_dict: dict
            dictionary of all bclass objects
        sec_adjustment_dict: dict
            dictionary with analyst adjustment information
        bloomberg_dict: dict
            dictionary of bloomberg information
        sdg_dict: dict
            dictionary of iss information
        """
        logging.log("Iterate Holdings")
        for index, row in self.df.iterrows():
            pf = row["Portfolio"]  # portfolio id
            isin = row["ISIN"]  # security isin

            security_store = securities[isin]
            parent_store = security_store.parent_store

            # attach information to security
            security_store.information["ESG_Collateral_Type"] = securitized_mapping[
                row["ESG Collateral Type"]
            ]
            security_store.information["Labeled_ESG_Type"] = row["Labeled ESG Type"]
            security_store.information["TCW_ESG"] = row["TCW ESG"]
            security_store.information["Issuer_ESG"] = row["Issuer ESG"]
            security_store.information["JPM_Sector"] = row["JPM Sector"]
            security_store.information["Country_of_Risk"] = row["Country of Risk"]
            security_store.information["BCLASS_Level2"] = row["BCLASS_Level2"]
            security_store.information["BCLASS_Level3"] = row["BCLASS_Level3"]
            if not pd.isna(row["ISSUER_NAME"]):
                security_store.information["Security_Name"] = row["ISSUER_NAME"]

            # attach information to security's company
            # create new objects for Muni, Sovereign and Securitized
            if row["Sector Level 2"] in ["Muni / Local Authority"]:
                self.create_store(
                    security_store=security_store,
                    check_type="Muni",
                    all_parents=self.munis,
                    companies=self.companies,
                )
            elif row["Sector Level 2"] in ["Residential MBS", "CMBS", "ABS"]:
                self.create_store(
                    security_store=security_store,
                    check_type="Securitized",
                    all_parents=self.securitized,
                    companies=self.companies,
                )
            elif row["Sector Level 2"] in ["Sovereign"]:
                self.create_store(
                    security_store=security_store,
                    check_type="Sovereign",
                    all_parents=self.sovereigns,
                    companies=self.companies,
                )
            parent_store = security_store.parent_store

            parent_store.information["Sector_Level_1"] = row["Sector Level 1"]
            parent_store.information["Sector_Level_2"] = row["Sector Level 2"]

            # attach BCLASS object
            self.attach_bclass(parent_store, row["BCLASS_Level4"], bclass_dict)

            # attach adjustment
            self.attach_analyst_adjustment(parent_store, isin, sec_adjustment_dict)

            # attach bloomberg information
            if row["BBG ISSUERID"] in bloomberg_dict:
                bbg_information = bloomberg_dict[row["BBG ISSUERID"]]
                parent_store.bloomberg_information = bbg_information
            else:
                bbg_information = deepcopy(bloomberg_dict[np.nan])
                if not hasattr(parent_store, "bloomberg_information"):
                    parent_store.bloomberg_information = bbg_information

            # attach iss information
            if row["ISS ISSUERID"] in sdg_dict:
                iss_information = sdg_dict[row["ISS ISSUERID"]]
                parent_store.sdg_information = iss_information
            else:
                iss_information = deepcopy(sdg_dict[np.nan])
                if not hasattr(parent_store, "sdg_information"):
                    parent_store.sdg_information = iss_information

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

    def create_store(
        self,
        security_store: secs.SecurityStore,
        check_type: str,
        all_parents: dict,
        companies: dict,
    ) -> None:
        """
        create new objects for Muni, Sovereign and Securitized if applicable

        Parameters
        ----------
        security_store: secs.SecurityStore
            security store
        check_type: str
            check if security is of this type, either Muni, Securitized or Sovereign
        all_parents: dict
            dictionary of current parent object of this type
        companies: dict
            dictionary of all company objects
        """
        parent_store = security_store.parent_store
        security_isin = security_store.information["Security ISIN"]
        issuer_isin = parent_store.isin
        class_ = mapping_configs.security_type_mapping[check_type]
        all_parents[issuer_isin] = all_parents.get(issuer_isin, class_(issuer_isin))
        parent_store.remove_security(security_isin)
        adj_df = parent_store.Adjustment
        msci_info = parent_store.msci_information
        if (not parent_store.securities) and parent_store.type == "company":
            companies.pop(issuer_isin, None)
        parent_store = all_parents[issuer_isin]
        parent_store.msci_information = msci_info
        parent_store.add_security(security_isin, security_store)
        parent_store.Adjustment = adj_df
        security_store.add_parent(parent_store)

    def attach_bclass(self, parent_store, bclass4: str, bclass_dict: dict) -> None:
        """
        Attach BCLASS object to security parent

        Parameters
        ----------
        parent_store: CompanyStore | MuniStore | SovereignStore | SecuritizedStore
            store object of parent
        bclass4: str
            BCLASS Level 4
        bclass_dict: dict
            dictionary of all bclass objects
        """
        # attach BCLASS object
        # if BCLASS is not in BCLASS store (covered industries), attach 'Unassigned BCLASS'
        if not bclass4 in bclass_dict:
            bclass_dict[bclass4] = sectors.BClass(
                bclass4,
                pd.Series(bclass_dict["Unassigned BCLASS"].information),
            )
            bclass_dict[bclass4].add_industry(bclass_dict["Unassigned BCLASS"].industry)
            bclass_dict["Unassigned BCLASS"].industry.add_sub_sector(
                bclass_dict[bclass4]
            )
        bclass_object = bclass_dict[bclass4]

        # for first initialization of BCLASS
        parent_store.information["BCLASS_Level4"] = parent_store.information.get(
            "BCLASS_Level4", bclass_object
        )
        # sometimes same security is labeled with different BCLASS_Level4
        # --> if it was unassigned before: overwrite, else: skipp
        if not (bclass_object.class_name == "Unassigned BCLASS"):
            parent_store.information["BCLASS_Level4"] = bclass_object
            bclass_object.companies[parent_store.isin] = parent_store

    def attach_analyst_adjustment(
        self,
        parent_store,
        security_isin: str,
        sec_adjustment_dict: dict,
    ) -> None:
        """
        Attach Analyst Adjustment to company
        Adjustment in sec_adjustment_dict is on security level

        Parameters
        ----------
        parent_store: CompanyStore | MuniStore | SovereignStore | SecuritizedStore
            store object of parent
        security_isin: str
            isin of security
        sec_adjustment_dict: dict
            dictionary with analyst adjustment information
        """
        if security_isin in sec_adjustment_dict:
            parent_store.Adjustment = sec_adjustment_dict[security_isin]

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
