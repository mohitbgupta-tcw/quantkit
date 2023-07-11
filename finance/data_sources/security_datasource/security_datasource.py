import quantkit.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.finance.companies.companies as comp


class SecurityDataSource(object):
    """
    Provide mapping on security level from SSI and MSCI

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        IssuerName: str
            name of issuer
        ISSUERID: str
            MSCI ID of issuer
        issID: int
            ISS ID of issuer
        Security ISIN: str
            isin of security
        Ticker: str
            ticker of issuer
        ISSUER_CUSIP: str
            cusip of issuer
        ISIN: str
            isin of issuer
        SECURITY_COUNTRY: str
            country of domicile of issuer (ISO2)
        Security Type: str
            security type (Equity or Fixed Income)
        CUSIP: str
            security cusip
        SEDOL: str
            security sedol
        CINS: str
            security cins
        ASSET_TYPE: str
            asset_type
        MARKET_STATUS: str
            market status
        EXCHANGE: str
            exchange

    """

    def __init__(self, params: dict):
        self.params = params
        self.iss = SecurityData(params["iss"])
        self.msci = SecurityData(params["msci"])

        self.all_tickers = list()
        self.securities = dict()
        self.equities = dict()
        self.fixed_income = dict()
        self.other_securities = dict()

    def load(self):
        """
        load data and transform dataframe
        """
        logging.log("Loading Security Data")
        self.msci.datasource.load()
        self.msci.transform_df()
        self.iss.datasource.load()
        self.iss.transform_df()
        self.transform_df()
        return

    def transform_df(self):
        """
        Several transformations
        """

        # Translate ISS Country into country name
        self.iss.datasource.df["CountryOfIncorporation"] = self.iss.datasource.df[
            "CountryOfIncorporation"
        ].replace("USA", "United States")

        # rename columns before merging
        self.iss.datasource.df = self.iss.datasource.df.rename(
            columns={"issuerID": "issID"}
        )

        # merge ISS and MSCI data on security level
        df_ = self.msci.datasource.df.merge(
            self.iss.datasource.df,
            how="outer",
            left_on="Client_ID",
            right_on="Security ISIN",
        )

        # take MSCI information, fill NA's with ISS information
        df_["IssuerName"] = df_["ISSUER_NAME"].fillna(df_["IssuerName"])
        df_["Security ISIN"] = df_["Client_ID"].fillna(df_["Security ISIN"])
        df_["Ticker"] = df_["ISSUER_TICKER"].fillna(df_["Ticker"])
        df_["ISSUERID"] = df_["ISSUERID"].fillna("NoISSUERID")
        df_["ISSUER_CNTRY_DOMICILE"] = df_["ISSUER_CNTRY_DOMICILE"].fillna(
            df_["CountryOfIncorporation"]
        )

        # drop duplicated columns
        df_ = df_.drop(
            [
                "ISSUER_NAME",
                "Client_ID",
                "ISSUER_TICKER",
                "CountryOfIncorporation",
            ],
            axis=1,
        )

        # add NoISIN row to df
        df_NoISIN = pd.DataFrame(
            {
                "ISIN": ["NoISIN"],
                "Security ISIN": ["NoISIN"],
                "IssuerName": ["NoISIN"],
                "ISSUERID": ["NoISSUERID"],
            }
        )
        df_ = pd.concat([df_, df_NoISIN], ignore_index=True)
        df_.reset_index()

        self.df_ = df_
        return

    def issuer_ids(self, securities: list):
        """
        For a list of isin's, return the corresponding MSCI ISSUERIDs

        Parameters
        ----------
        securities: list
            list of all security isins in portfolios

        Returns
        -------
        list
            list of all msci issuer ids for isin's in security list
        """
        df_ = self.df[self.df["Security ISIN"].isin(securities)]
        ii = list(df_["ISSUERID"].unique())
        ii.remove("NoISSUERID")
        return ii

    def iter(
        self,
        securities: list,
        companies: dict,
        df_portfolio: pd.DataFrame,
        msci_df: pd.DataFrame,
        adjustment_df: pd.DataFrame,
    ):
        """
        - create Company object for each security with key ISIN
        - create Security object with key Security ISIN
        - attach analyst adjustment based on sec isin
        - attach msci information to company

        Parameters
        ----------
        securities: list
            list of all security isins in portfolios
        companies: dict
            dictionary of all company objects
        df_portfolio: pd.DataFrame
            portfolio DataFrame with all holdings
        msci_df: pd.DataFrame
            DataFrame with all MSCI information
        adjustment_df: pd.DataFrame
            DataFrame with analyst adjustment information
        """
        df_ = self.df[self.df["Security ISIN"].isin(securities)]

        for sec in securities:
            security_row = df_[df_["Security ISIN"] == sec]

            # we have information for security in our database
            if not security_row.empty:
                sec_info = security_row.squeeze().to_dict()
                if pd.isna(sec_info["ISIN"]):
                    sec_info["ISIN"] = sec
            # no information about security in database --> get information from portfoliot tab
            else:
                portfolio_row = df_portfolio[df_portfolio["ISIN"] == sec]
                sec_info = security_row.reindex(list(range(1))).squeeze().to_dict()
                sec_info["Security ISIN"] = sec
                sec_info["ISIN"] = sec
                sec_info["ISSUERID"] = "NoISSUERID"
                sec_info["IssuerName"] = portfolio_row["ISSUER_NAME"].values[0]
                sec_info["Ticker"] = portfolio_row["Ticker Cd"].values[0]

            # get MSCI information of parent issuer
            issuer_id = sec_info["ISSUERID"]
            msci_row = msci_df[msci_df["ISSUERID"] == issuer_id]
            # issuer has msci information --> overwrite security information
            if not msci_row.empty:
                issuer_isin = msci_row["ISSUER_ISIN"].values[0]
                if not pd.isna(issuer_isin):
                    sec_info["ISIN"] = issuer_isin
                    sec_info["IssuerName"] = msci_row["ISSUER_NAME"].values[0]
                    sec_info["Ticker"] = msci_row["ISSUER_TICKER"].values[0]
                msci_information = msci_row.squeeze().to_dict()
            else:
                msci_information = msci_row.reindex(list(range(1))).squeeze().to_dict()
                msci_information["ISSUER_NAME"] = sec_info["IssuerName"]
                msci_information["ISSUER_ISIN"] = sec_info["ISIN"]
                msci_information["ISSUER_TICKER"] = sec_info["Ticker"]

            # append to all ticker list
            self.all_tickers.append(sec_info["Ticker"])

            # create security store --> seperate Fixed Income and Equity stores based on Security Type
            # if Security Type is NA, just create Security object
            sec_type = sec_info["Security Type"]
            class_ = mapping_configs.security_store_mapping[sec_type]
            type_mapping = mapping_configs.security_mapping[sec_type]
            security_store = class_(isin=sec, information=sec_info)
            self.securities[sec] = security_store
            getattr(self, type_mapping)[sec] = self.securities[sec]

            # create company object
            # company object could already be there if company has more than 1 security --> get
            issuer = sec_info["ISIN"]
            companies[issuer] = companies.get(
                issuer,
                comp.CompanyStore(
                    isin=issuer,
                    row_data=msci_information,
                ),
            )

            # attach security to company and vice versa
            companies[issuer].add_security(sec, self.securities[sec])
            self.securities[sec].parent_store = companies[issuer]

            # attach adjustment
            adj_df = adjustment_df[adjustment_df["ISIN"] == sec]
            if not adj_df.empty:
                companies[issuer].Adjustment = pd.concat(
                    [companies[issuer].Adjustment, adj_df],
                    ignore_index=True,
                    sort=False,
                )
        return

    @property
    def df(self):
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.df_


class SecurityData(ds.DataSources):
    """
    Wrapper to load SSI and MSCI data

    Parameters
    ----------
    params: dict
        datasource specific parameters including datasource
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def transform_df(self):
        """
        None
        """
        return
