import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np


class SecurityDataSource(object):
    """
    Provide information on security level from SSI and MSCI

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        IssuerName: str
            name of issuer
        issuerID: int
            ID of issuer
        Security ISIN: str
            isin of security
        Ticker: str
            ticker of issuer
        ISSUER_CUSIP: str
            cusip of issuer
        ISIN: str
            isin of issuer
        ISSUER_CNTRY_DOMICILE: str
            country of domicile of issuer (ISO2)
        Security Type: str
            security type (Equity or Fixed Income)
        MSCI ESG measures: str | float
            ESG related measures
    """

    def __init__(self, params: dict):
        logging.log("Loading Security Data")
        self.params = params
        self.ssi = SecurityData(params["ssi"])
        self.msci = SecurityData(params["msci"])
        self.df_ = self.transform_df()

    def transform_df(self):
        """
        Several transformations
        """

        # Translate SSI Country into country name
        self.ssi.datasource.df["CountryOfIncorporation"] = self.ssi.datasource.df[
            "CountryOfIncorporation"
        ].replace("USA", "United States")

        # merge SSI and MSCI data on security level
        df_ = self.msci.datasource.df.merge(
            self.ssi.datasource.df,
            how="outer",
            left_on="Client_ID",
            right_on="Security ISIN",
        )

        # take SSI information, fill NA's with MSCI information
        df_["IssuerName"] = df_["IssuerName"].fillna(df_["ISSUER_NAME"])
        df_["Security ISIN"] = df_["Security ISIN"].fillna(df_["Client_ID"])
        df_["Ticker"] = df_["Ticker"].fillna(df_["ISSUER_TICKER"])
        df_["ISIN"] = df_["ISIN"].fillna(df_["ISSUER_ISIN"])
        df_["ISSUER_CNTRY_DOMICILE"] = df_["ISSUER_CNTRY_DOMICILE"].fillna(
            df_["CountryOfIncorporation"]
        )

        # drop duplicated columns
        df_ = df_.drop(
            [
                "ISSUER_NAME",
                "Client_ID",
                "ISSUER_TICKER",
                "ISSUER_ISIN",
                "CountryOfIncorporation",
            ],
            axis=1,
        )

        # fill GICS industry NA's with 'Unassigned GICS'
        df_["GICS_SUB_IND"] = df_["GICS_SUB_IND"].fillna("Unassigned GICS")

        # replace values in each column from params transformation file
        df_ = df_.replace(self.params["transformation"])

        # add NoISIN row to df
        df_NoISIN = pd.DataFrame(
            {
                "Security ISIN": ["NoISIN"],
                "ISIN": ["NoISIN"],
                "IssuerName": ["NoISIN"],
                "GICS_SUB_IND": ["Unassigned GICS"],
            }
        )
        df_ = pd.concat([df_, df_NoISIN], ignore_index=True)
        df_.reset_index()

        return df_

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
