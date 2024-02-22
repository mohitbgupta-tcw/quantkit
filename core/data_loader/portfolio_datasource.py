import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.core.financial_infrastructure.portfolios as portfolios
import quantkit.core.financial_infrastructure.securities as securities
import quantkit.handyman.msci_data_loader as msci_data_loader
import quantkit.core.financial_infrastructure.companies as comp
import quantkit.core.financial_infrastructure.munis as munis
import quantkit.core.financial_infrastructure.securitized as securitized
import quantkit.core.financial_infrastructure.sovereigns as sovereigns
import quantkit.core.financial_infrastructure.cash as cash
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
        ISIN: str
            isin of security
        Security_Name: str
            name of security
        Ticker Cd: str
            ticker of issuer
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
        query: str,
    ) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        query: str
            SQL query
        """
        logging.log("Loading Portfolio Data")
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - replace NA's in ISIN with Name of Cash
        - change first letter of each word to upper, else lower case
        - replace NA's in several columns
        - replace NA's of MSCI ISSUERID by running MSCI API
        - reaplace transformation values
        """
        self.datasource.df["As Of Date"] = pd.to_datetime(
            self.datasource.df["As Of Date"]
        )
        self.datasource.df["Portfolio_Weight"] = self.datasource.df[
            "Portfolio_Weight"
        ].astype(float)
        self.datasource.df["Base Mkt Val"] = self.datasource.df["Base Mkt Val"].astype(
            float
        )

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
        self.datasource.df["ISIN"] = np.where(
            self.datasource.df["ISIN"] == " ",
            self.datasource.df["Security_Name"],
            self.datasource.df["ISIN"],
        )
        self.datasource.df["Security_Name"].fillna(
            self.datasource.df["ISIN"], inplace=True
        )
        self.datasource.df["Issuer ISIN"].fillna(
            self.datasource.df["ISIN"], inplace=True
        )
        self.datasource.df["Ticker Cd"] = self.datasource.df["Ticker Cd"].replace(
            to_replace="/", value=".", regex=True
        )

        sec_isins = list(
            self.datasource.df[self.datasource.df["MSCI ISSUERID"].isna()]["ISIN"]
            .dropna()
            .unique()
        )
        if sec_isins:
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

    def iter_holdings(
        self,
        msci_dict: dict,
        security_type_mapping: dict = {
            "Securitized": securitized.SecuritizedStore,
            "Muni": munis.MuniStore,
            "Sovereign": sovereigns.SovereignStore,
            "Cash": cash.CashStore,
            "Corporate": comp.CompanyStore,
        },
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
        security_type_mapping: dict, optional
            dictionary of security types
        """
        logging.log("Iterate Holdings")

        drop_df = self.df.drop_duplicates(subset=["ISIN"], keep="first")
        for index, row in drop_df.iterrows():
            isin = row["ISIN"]
            sec_info = row.to_dict()

            msci_information = self.create_msci_information(sec_info, msci_dict)

            self.create_security_store(isin, sec_info)
            security_store = self.securities[isin]

            issuer = sec_info["Issuer ISIN"]

            self.create_parent_store(
                sector_level_2=sec_info["Sector Level 2"],
                issuer_isin=issuer,
                msci_information=msci_information,
                security_store=security_store,
                security_type_mapping=security_type_mapping,
            )

        self.attach_securities()

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

    def create_parent_store(
        self,
        sector_level_2: str,
        issuer_isin: str,
        msci_information: dict,
        security_store: securities.SecurityStore,
        security_type_mapping: dict,
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
        security_type_mapping: dict
            dictionary of security types
        """
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
