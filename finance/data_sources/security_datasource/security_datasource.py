import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd
import numpy as np
import quantkit.utils.dataframe_utils as dataframe_utils


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

        # # delete duplicated rows and take mode for ticker
        # self.msci.datasource.df =  dataframe_utils.group_mode(self.msci.datasource.df, "Client_ID", "ISSUER_TICKER")

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
