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

    def __init__(self, params: dict, **kwargs) -> None:
        self.params = params
        self.iss = SecurityData(params["iss"], **kwargs)
        self.msci = SecurityData(params["msci"], **kwargs)

        self.all_tickers = list()
        self.securities = dict()
        self.equities = dict()
        self.fixed_income = dict()
        self.other_securities = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Security Data")
        self.msci.load()
        self.iss.load()
        self.transform_df()

    def transform_df(self) -> None:
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

    def issuer_ids(self, securities: list) -> list:
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
    ) -> None:
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

            # get MSCI information
            msci_information = self.create_msci_information(sec_info, msci_df)

            # append to all ticker list
            self.all_tickers.append(sec_info["Ticker"])

            # create security store
            self.create_security_store(sec, sec_info)

            # create company object
            issuer = sec_info["ISIN"]
            self.create_company_store(issuer, companies, msci_information)

            # attach security to company and vice versa
            companies[issuer].add_security(sec, self.securities[sec])
            self.securities[sec].parent_store = companies[issuer]

            # attach adjustment
            self.attach_analyst_adjustment(issuer, sec, companies, adjustment_df)

    def create_security_store(
        self, security_isin: str, security_information: dict
    ) -> None:
        """
        create security store
        --> seperate Fixed Income and Equity stores based on Security Type
        if Security Type is NA, just create Security object

        Parameters
        ----------
        security isin: str
            isin of security
        security_information: dict
            dictionary of information about the security
        """
        sec_type = security_information["Security Type"]
        class_ = mapping_configs.security_store_mapping[sec_type]
        type_mapping = mapping_configs.security_mapping[sec_type]
        security_store = class_(isin=security_isin, information=security_information)
        self.securities[security_isin] = security_store
        getattr(self, type_mapping)[security_isin] = self.securities[security_isin]

    def create_company_store(
        self, company_isin: str, companies: dict, msci_information: dict
    ) -> None:
        """
        Create company object with msci information

        Parameters
        ----------
        company_isin: str
            isin of company
        companies: dict
            dictionary of all company objects
        msci_information: dict
            dictionary of msci information on company level
        """
        # company object could already be there if company has more than 1 security
        if company_isin in companies:
            # assign msci data for missing values
            for val in msci_information:
                if pd.isna(companies[company_isin].msci_information[val]):
                    companies[company_isin].msci_information[val] = msci_information[
                        val
                    ]
        else:
            companies[company_isin] = comp.CompanyStore(
                isin=company_isin,
                row_data=msci_information,
            )

    def create_msci_information(
        self, security_information: dict, msci_df: pd.DataFrame
    ) -> dict:
        """
        Get MSCI information of company

        Parameters
        ----------
        security_information: dict
            dictionary of information about the security
        msci_df: pd.DataFrame
            DataFrame with all MSCI information

        Returns
        -------
        dict
            dictionary of MSCI information
        """
        issuer_id = security_information["ISSUERID"]
        msci_row = msci_df[msci_df["ISSUERID"] == issuer_id]
        # issuer has msci information --> overwrite security information
        if not msci_row.empty:
            issuer_isin = msci_row["ISSUER_ISIN"].values[0]
            if not pd.isna(issuer_isin):
                security_information["ISIN"] = issuer_isin
                security_information["IssuerName"] = msci_row["ISSUER_NAME"].values[0]
            else:
                msci_row["ISSUER_ISIN"] = security_information["ISIN"]
            if not pd.isna(msci_row["ISSUER_TICKER"].values[0]):
                security_information["Ticker"] = msci_row["ISSUER_TICKER"].values[0]
            else:
                msci_row["ISSUER_TICKER"] = security_information["Ticker"]
            msci_information = msci_row.squeeze().to_dict()
        else:
            msci_information = msci_row.reindex(list(range(1))).squeeze().to_dict()
            msci_information["ISSUER_NAME"] = security_information["IssuerName"]
            msci_information["ISSUER_ISIN"] = security_information["ISIN"]
            msci_information["ISSUER_TICKER"] = security_information["Ticker"]
        return msci_information

    def attach_analyst_adjustment(
        self,
        company_isin: str,
        security_isin: str,
        companies: dict,
        adjustment_df: pd.DataFrame,
    ) -> None:
        """
        Attach Analyst Adjustment to company
        Adjustment in adjustment_df is on security level

        Parameters
        ----------
        company_isin: str
            isin of company
        security_isin: str
            isin of security
        companies: dict
            dictionary of all company objects
        adjustment_df: pd.DataFrame
            DataFrame with analyst adjustment information
        """
        adj_df = adjustment_df[adjustment_df["ISIN"] == security_isin]
        if not adj_df.empty:
            companies[company_isin].Adjustment = pd.concat(
                [companies[company_isin].Adjustment, adj_df],
                ignore_index=True,
                sort=False,
            )

    @property
    def df(self) -> pd.DataFrame:
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

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        from_table = (
            from_table
        ) = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        None
        """
        pass
