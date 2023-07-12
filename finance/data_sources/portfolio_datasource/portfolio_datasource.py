import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.finance.portfolios.portfolios as portfolios
import quantkit.finance.companies.companies as comp
import quantkit.finance.securities.securities as secs
import quantkit.finance.sectors.sectors as sectors
import quantkit.utils.mapping_configs as mapping_configs
import pandas as pd
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
        ISSUER_Name: str
            name of issuer
        TCW ESG: str
            ESG label of TCW
        Ticker Cd: str
            ticker of issuer
        Sector Level 1: str
            sector 1 of issuer
        Sector Level 2: str
            sector 2 of issuer
        BCLASS_level4: str
            BClass of issuer
        Portfolio_Weight: float
            weight of security in portfolio
        Base Mkt Value: float
            market value of position in portfolio
        Rating Raw MSCI: str
            MSCI rating of issuer
        OAS: float
            OAS
    """

    def __init__(self, params: dict):
        super().__init__(params)
        self.portfolios = dict()
        self.companies = dict()
        self.munis = dict()
        self.sovereigns = dict()
        self.securitized = dict()

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Portfolio Data")
        self.datasource.load()
        self.transform_df()
        return

    def transform_df(self):
        """
        - replace NA's in ISIN with 'NoISIN'
        - replace NA's in BCLASS_Level4 with 'Unassigned BCLASS'
        - change first letter of each word to upper, else lower case
        """
        self.datasource.df["ISIN"].fillna("NoISIN", inplace=True)
        self.datasource.df["ISIN"].replace("--", "NoISIN", inplace=True)
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].fillna("Unassigned BCLASS")
        self.datasource.df["BCLASS_Level4"].replace(
            "N/A", "Unassigned BCLASS", inplace=True
        )
        self.datasource.df["BCLASS_Level4"] = self.datasource.df[
            "BCLASS_Level4"
        ].str.title()
        self.datasource.df["BCLASS_Level4"].replace(
            "Unassigned Bclass", "Unassigned BCLASS", inplace=True
        )
        return

    def iter(self):
        """
        iterate over portfolios and:
        - Create Portfolio Objects
        - Save in self.portfolios
        - key is portfolio id
        """
        for index, row in (
            self.df[["Portfolio", "Portfolio Name"]].drop_duplicates().iterrows()
        ):
            pf = row["Portfolio"]
            pf_store = portfolios.PortfolioStore(pf=pf, name=row["Portfolio Name"])
            holdings_df = self.df[self.df["Portfolio"] == pf][
                ["As Of Date", "ISIN", "Portfolio_Weight", "Base Mkt Val", "OAS"]
            ]
            pf_store.add_holdings(holdings_df)
            self.portfolios[pf] = pf_store

        # save all securities that occur in portfolios to filter down security database later on
        self.all_holdings = list(self.df["ISIN"].unique())
        self.all_holdings.append(
            "NoISIN"
        ) if "NoISIN" not in self.all_holdings else self.all_holdings

        return

    def iter_holdings(
        self, securities: dict, securitized_mapping: dict, bclass_dict: dict
    ):
        """
        Iterate over portfolio holdings
        - attach ESG information so security
        - create Muni, Sovereign, Securitized objects
        - attach sector information to company
        - attach BCLASS to company
        - attach MSCI rating to company
        - attach holdings, OAS to self.holdings with security object

        Parameters
        ----------
        securities: dict
            dictionary of all security objects
        securitized_mapping: dict
            mapping for ESG Collat Type
        bclass_dict: dict
            dictionary of all bclass objects
        """
        logging.log("Iterate Holdings")
        for index, row in self.df.iterrows():
            pf = row["Portfolio"]  # portfolio id
            isin = row["ISIN"]  # security isin
            # no ISIN for security (cash, swaps, etc.)
            # --> create company object with name as identifier
            if isin == "NoISIN":
                if pd.isna(row["ISSUER_NAME"]):
                    isin = "Cash"
                else:
                    isin = row["ISSUER_NAME"]
                self.companies[isin] = self.companies.get(
                    isin,
                    comp.CompanyStore(
                        isin,
                        deepcopy(self.companies["NoISIN"].msci_information),
                    ),
                )
                self.companies[isin].msci_information["ISSUER_NAME"] = isin
                self.companies[isin].msci_information["ISSUER_TICKER"] = row[
                    "Ticker Cd"
                ]
                # create security object if not there yet
                securities[isin] = securities.get(
                    isin,
                    secs.SecurityStore(
                        isin,
                        {
                            "ISSUERID": "NoISSUERID",
                            "Security ISIN": isin,
                            "ISIN": isin,
                            "IssuerName": isin,
                            "Security_Name": isin,
                        },
                    ),
                )
                # assign parent store and vice versa
                securities[isin].add_parent(self.companies[isin])
                self.companies[isin].add_security(isin, securities[isin])

            security_store = securities[isin]
            parent_store = security_store.parent_store

            # attach information to security
            security_store.information["ESG_Collateral_Type"] = securitized_mapping[
                row["ESG Collateral Type"]
            ]
            security_store.information["Labeled_ESG_Type"] = row["Labeled ESG Type"]
            security_store.information["TCW_ESG"] = row["TCW ESG"]
            security_store.information["Issuer_ESG"] = row["Issuer ESG"]
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

            # attach MSCI rating
            self.attach_msci_rating(parent_store, row["Rating Raw MSCI"])

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

        self.companies["NoISIN"].information["BCLASS_Level4"] = bclass_dict[
            "Unassigned BCLASS"
        ]
        return

    def create_store(
        self,
        security_store: secs.SecurityStore,
        check_type: str,
        all_parents: dict,
        companies: dict,
    ):
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
        return

    def attach_bclass(self, parent_store, bclass4: str, bclass_dict: dict):
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
        bclass_dict[bclass4] = bclass_dict.get(
            bclass4,
            sectors.BClass(
                bclass4,
                pd.Series(bclass_dict["Unassigned BCLASS"].information),
            ),
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
        return

    def attach_msci_rating(self, parent_store, msci_rating):
        """
        Attach MSCI Rating to security parent

        Parameters
        ----------
        parent_store: CompanyStore | MuniStore | SovereignStore | SecuritizedStore
            store object of parent
        """
        # for first initialization of MSCI Rating
        parent_store.information["Rating_Raw_MSCI"] = parent_store.information.get(
            "Rating_Raw_MSCI", msci_rating
        )

        # sometimes same security is labeled with different MSCI Ratings
        # --> if it's not NA: overwrite, else: skipp
        if not pd.isna(msci_rating):
            parent_store.information["Rating_Raw_MSCI"] = msci_rating
        return

    @property
    def df(self):
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
