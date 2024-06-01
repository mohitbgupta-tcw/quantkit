import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.core.financial_infrastructure.portfolios as portfolios
import quantkit.core.financial_infrastructure.securities as securities
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
        DATE: datetime.date
            date of portfolio
        PORTFOLIO: str
            portfolio id
        PORTFOLIO_NAME: str
            portfolio name
        SECURITY_KEY: str
            key of security
        PORTFOLIO_WEIGHT: float
            weight of security in portfolio
        BASE_MKT_VAL: float
            market value of position in portfolio
        OAS: float
            OAS
    """

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.tickers = dict()
        self.portfolios = dict()
        self.securities = dict()
        self.companies = dict()
        self.munis = dict()
        self.sovereigns = dict()
        self.securitized = dict()
        self.cash = dict()

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

        logging.log("Loading Portfolio Data")
        query = f"""
        SELECT *
        FROM (
            SELECT 
                pos.as_of_date as "DATE",
                pos.portfolio_number AS "PORTFOLIO",
                pos.portfolio_name AS "PORTFOLIO_NAME",
                pos.security_key AS "SECURITY_KEY",
                pos.market_value_percent * 100 AS "PORTFOLIO_WEIGHT",
                pos.base_market_value AS "BASE_MKT_VAL",
                null AS "OAS",
            FROM tcw_core.tcw.position_vw pos
            JOIN tcw_core.tcw.portfolio_vw strat 
                ON pos.portfolio_key = strat.portfolio_key 
                AND pos.as_of_date = strat.as_of_date 
                AND strat.is_active = 1
                AND strat.portfolio_type_1 IN ('Trading', 'Reporting')
            WHERE pos.as_of_date >= '{start_date}'
            AND pos.as_of_date <= '{end_date}'
            {and_clause}
            UNION ALL
            --Benchmark Holdings
            SELECT  
                bench.as_of_date AS "DATE",
                bench.benchmark_name AS "PORTFOLIO",
                bench.benchmark_name AS "PORTFOLIO_NAME", 
                sec.security_key AS "SECURITY_KEY",
                CASE 
                    WHEN bench.market_value_percentage IS null 
                    THEN bench.market_value / SUM(bench.market_value)
                        OVER(partition BY bench.benchmark_name) 
                    ELSE bench.market_value_percentage  
                END AS "PORTFOLIO_WEIGHT",
                bench.market_value AS "BASE_MKT_VAL",
                null AS "OAS",
            FROM tcw_core.benchmark.benchmark_position_vw bench
            LEFT JOIN tcw_core.tcw.security_vw sec 
                ON bench.security_key = sec.security_key
                AND bench.as_of_date = sec.as_of_date
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
        ORDER BY "PORTFOLIO" ASC, "DATE" ASC, "PORTFOLIO_WEIGHT" DESC
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - replace NA's in several columns
        - change column types
        """
        self.datasource.df["DATE"] = pd.to_datetime(self.datasource.df["DATE"])
        self.datasource.df["PORTFOLIO_WEIGHT"] = self.datasource.df[
            "PORTFOLIO_WEIGHT"
        ].astype(float)
        self.datasource.df["BASE_MKT_VAL"] = self.datasource.df["BASE_MKT_VAL"].astype(
            float
        )

        self.datasource.df.replace("N/A", np.nan, inplace=True)
        self.datasource.df = self.datasource.df.fillna(value=np.nan)

    def iter(self) -> None:
        """
        - iterate over portfolios and:
            - Create Portfolio Objects
            - Save in self.portfolios
            - key is portfolio id
            - add holdings df
        """
        self.security_keys = list(self.df["SECURITY_KEY"].unique())
        for index, row in (
            self.df[["PORTFOLIO", "PORTFOLIO_NAME"]].drop_duplicates().iterrows()
        ):
            pf = row["PORTFOLIO"]
            pf_store = portfolios.PortfolioStore(pf=pf, name=row["PORTFOLIO_NAME"])
            holdings_df = self.df[self.df["PORTFOLIO"] == pf]
            pf_store.add_holdings(holdings_df)
            as_of_date = (
                self.df[self.df["PORTFOLIO"] == pf]
                .groupby("PORTFOLIO")["DATE"]
                .max()
                .values[0]
            )
            pf_store.add_as_of_date(as_of_date)
            self.portfolios[pf] = pf_store

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

    def attach_securities(self) -> None:
        """
        Attach security object, portfolio weight, OAS to portfolios

        Parameters
        ----------
        portfolio_rows: pd.DataFrame
            DataFrane of portfolios security is held in
        isin: str
            security isin
        """
        group_df = (
            self.df.groupby(["Portfolio", "ISIN"])
            .agg(
                {
                    "As Of Date": "last",
                    "Portfolio_Weight": "sum",
                    "Base Mkt Val": "sum",
                    "OAS": "last",
                }
            )
            .reset_index(level=1)
        )

        for pf, row in group_df.iterrows():
            isin = row["ISIN"]
            holding_measures = row.to_dict()

            security_store = self.securities[isin]

            self.portfolios[pf].holdings[isin] = {
                "object": security_store,
                "holding_measures": holding_measures,
            }

            # attach portfolio to security
            security_store.portfolio_store[pf] = self.portfolios[pf]

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
