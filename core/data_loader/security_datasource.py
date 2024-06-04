import quantkit.core.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import quantkit.core.financial_infrastructure.issuer as issuer
import quantkit.core.financial_infrastructure.securities as securities
import pandas as pd
import numpy as np


class SecurityDataSource(ds.DataSources):
    """
    Provide security data

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
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
        self.securities = dict()
        self.issuers = dict()

    def load(
        self,
        securities: list,
        start_date: str,
        end_date: str,
    ) -> None:
        """
        load data and transform dataframe

        Parameters
        ----------
        securities: list
            list of security keys
        start_date: str
            start date to pull from API
        end_date: str
            end date to pull from API
        """
        logging.log("Loading Security Data")
        security_string = ", ".join(f"'{s}'" for s in securities)
        query = f"""
            SELECT *
            FROM (
                SELECT 
                    sec.as_of_date AS "DATE",
                    sec.security_key AS "SECURITY_KEY",
                    sec.isin AS "ISIN",
                    sec.cusip AS "CUSIP",
                    sec.ticker AS "TICKER",
                    sec.security_name AS "SECURITY_NAME",
                    sec.tclass_level1 AS "SECURITY_TYPE",
                    sec.issuer_id_msci AS "MSCI_ISSUERID",
                    sec.issuer_id_bbg AS "BBG_ISSUERID",
                    sec.issuer_id_iss "ISS_ISSUERID",
                    sec.issuer_id_aladdin AS "ALADDIN_ISSUERID",
                    CASE 
                        WHEN rs.rclass1_name IS null 
                        THEN 'Cash and Other' 
                        ELSE  rs.rclass1_name 
                    END AS "SECTOR_LEVEL_1",
                    CASE 
                        WHEN rs.rclass2_name IS null 
                        THEN 'Cash and Other' 
                        ELSE  rs.rclass2_name 
                    END AS "SECTOR_LEVEL_2",
                    sec.loan_category AS "LOAN_CATEGORY",
                    sec.labeled_esg_type AS "LABELED_ESG_TYPE",
                    CASE 
                        WHEN sec.issuer_esg ='NA '
                        OR sec.issuer_esg IS null
                        THEN 'No' 
                        ELSE sec.issuer_esg 
                    END AS "ISSUER_ESG",
                    sec.tcw_esg_type AS "TCW_ESG",
                    sec.esg_collateral_type AS "ESG_COLLATERAL_TYPE",
                    CASE 
                        WHEN sec.EM_COUNTRY_OF_RISK_NAME IS null 
                        THEN sec.COUNTRY_OF_RISK_NAME
                        ELSE sec.EM_COUNTRY_OF_RISK_NAME
                    END AS "COUNTRY_OF_RISK",
                    CASE 
                        WHEN sec.EM_COUNTRY_OF_RISK_CODE IS null 
                        THEN sec.COUNTRY_OF_RISK_CODE 
                        ELSE sec.EM_COUNTRY_OF_RISK_CODE 
                    END AS "ISO2",
                    sec.jpm_level1 AS "JPM_SECTOR",
                    sec.bclass_level2_name AS "BCLASS_LEVEL2", 
                    sec.bclass_level3_name AS "BCLASS_LEVEL3", 
                    sec.bclass_level4_name AS "BCLASS_LEVEL4",
                    sec.gics_level2_name AS "GICS_LEVEL2", 
                    sec.gics_level3_name AS "GICS_LEVEL3", 
                    sec.gics_level4_name AS "GICS_LEVEL4",
                FROM tcw_core.tcw.security_vw sec 
                LEFT JOIN tcw_core.reference.rclass_mapped_sectors_vw rs 
                    ON sec.sclass_key  = rs.sclass_sector_key
                    AND rs.rclass_scheme_name = '7. ESG - Primary Summary'
                WHERE sec.as_of_date >= '{start_date}'
                AND sec.as_of_date <= '{end_date}'
                AND sec.security_key in ({security_string})
            ) 
            ORDER BY "DATE" ASC, "TICKER" ASC
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        - replace NA's in several columns
        - change column types
        - replace missing ID's with ID's from same issuer
        - create issuer ID
        """
        self.datasource.df["DATE"] = pd.to_datetime(self.datasource.df["DATE"])
        self.datasource.df = self.datasource.df.replace(["None", "N/A", " "], np.nan)
        self.datasource.df = self.datasource.df.fillna(value=np.nan)

        if self.params.get("transformation"):
            for sec, trans in self.params["transformation"].items():
                for col, col_value in trans.items():
                    self.datasource.df.loc[
                        self.datasource.df["ISIN"] == sec, col
                    ] = col_value

        self.datasource.df["ESG_COLLATERAL_TYPE"] = (
            self.datasource.df["ESG_COLLATERAL_TYPE"]
            .str.strip()
            .fillna(value="Unknown")
        )
        self.datasource.df.loc[
            (self.datasource.df["ISIN"].isna())
            & self.datasource.df["SECURITY_NAME"].isna(),
            ["ISIN", "SECURITY_NAME"],
        ] = "Cash"
        self.datasource.df["ISIN"] = self.datasource.df["ISIN"].fillna(
            self.datasource.df["SECURITY_NAME"]
        )
        self.datasource.df["BBG_ISSUERID"] = self.datasource.df["BBG_ISSUERID"].astype(
            "string"
        )
        self.datasource.df["ISS_ISSUERID"] = self.datasource.df["ISS_ISSUERID"].astype(
            "string"
        )
        self.datasource.df["ALADDIN_ISSUERID"] = self.datasource.df[
            "ALADDIN_ISSUERID"
        ].astype("string")

        name_dict = {
            "MSCI_ISSUERID": "_msci",
            "BBG_ISSUERID": "_bbg",
            "ISS_ISSUERID": "_iss",
            "ALADDIN_ISSUERID": "_aladdin",
        }
        for col in list(name_dict.keys()):
            replace_df = self.datasource.df[
                ["MSCI_ISSUERID", "BBG_ISSUERID", "ISS_ISSUERID", "ALADDIN_ISSUERID"]
            ]
            replace_df = replace_df.dropna(subset=col)
            replace_df = replace_df.convert_dtypes()
            replace_df = replace_df.groupby([col]).agg(
                {
                    "MSCI_ISSUERID": "max",
                    "BBG_ISSUERID": "max",
                    "ISS_ISSUERID": "max",
                    "ALADDIN_ISSUERID": "max",
                }
            )
            replace_df = replace_df.drop(col, axis=1).reset_index()
            self.datasource.df = self.datasource.df.merge(
                right=replace_df,
                left_on=col,
                right_on=col,
                suffixes=["", name_dict[col]],
                how="left",
            )

        self.datasource.df["MSCI_ISSUERID"] = self.datasource.df[
            "MSCI_ISSUERID"
        ].fillna(self.datasource.df["MSCI_ISSUERID_bbg"])
        self.datasource.df["MSCI_ISSUERID"] = self.datasource.df[
            "MSCI_ISSUERID"
        ].fillna(self.datasource.df["MSCI_ISSUERID_iss"])
        self.datasource.df["MSCI_ISSUERID"] = self.datasource.df[
            "MSCI_ISSUERID"
        ].fillna(self.datasource.df["MSCI_ISSUERID_aladdin"])
        self.datasource.df["BBG_ISSUERID"] = self.datasource.df["BBG_ISSUERID"].fillna(
            self.datasource.df["BBG_ISSUERID_msci"]
        )
        self.datasource.df["BBG_ISSUERID"] = self.datasource.df["BBG_ISSUERID"].fillna(
            self.datasource.df["BBG_ISSUERID_iss"]
        )
        self.datasource.df["BBG_ISSUERID"] = self.datasource.df["BBG_ISSUERID"].fillna(
            self.datasource.df["BBG_ISSUERID_aladdin"]
        )
        self.datasource.df["ISS_ISSUERID"] = self.datasource.df["ISS_ISSUERID"].fillna(
            self.datasource.df["ISS_ISSUERID_bbg"]
        )
        self.datasource.df["ISS_ISSUERID"] = self.datasource.df["ISS_ISSUERID"].fillna(
            self.datasource.df["ISS_ISSUERID_msci"]
        )
        self.datasource.df["ISS_ISSUERID"] = self.datasource.df["ISS_ISSUERID"].fillna(
            self.datasource.df["ISS_ISSUERID_aladdin"]
        )
        self.datasource.df["ALADDIN_ISSUERID"] = self.datasource.df[
            "ALADDIN_ISSUERID"
        ].fillna(self.datasource.df["ALADDIN_ISSUERID_bbg"])
        self.datasource.df["ALADDIN_ISSUERID"] = self.datasource.df[
            "ALADDIN_ISSUERID"
        ].fillna(self.datasource.df["ALADDIN_ISSUERID_msci"])
        self.datasource.df["ALADDIN_ISSUERID"] = self.datasource.df[
            "ALADDIN_ISSUERID"
        ].fillna(self.datasource.df["ALADDIN_ISSUERID_iss"])
        self.datasource.df = self.datasource.df.drop(
            [
                "MSCI_ISSUERID_bbg",
                "MSCI_ISSUERID_iss",
                "MSCI_ISSUERID_aladdin",
                "BBG_ISSUERID_msci",
                "BBG_ISSUERID_iss",
                "BBG_ISSUERID_aladdin",
                "ISS_ISSUERID_bbg",
                "ISS_ISSUERID_msci",
                "ISS_ISSUERID_aladdin",
                "ALADDIN_ISSUERID_bbg",
                "ALADDIN_ISSUERID_msci",
                "ALADDIN_ISSUERID_iss",
            ],
            axis=1,
        )
        self.datasource.df["ISSUER_ID"] = np.where(
            ~self.datasource.df["MSCI_ISSUERID"].isna(),
            self.datasource.df["MSCI_ISSUERID"],
            np.where(
                ~self.datasource.df["BBG_ISSUERID"].isna(),
                self.datasource.df["BBG_ISSUERID"] + "_bbg",
                np.where(
                    ~self.datasource.df["ISS_ISSUERID"].isna(),
                    self.datasource.df["ISS_ISSUERID"] + "_iss",
                    np.where(
                        ~self.datasource.df["ALADDIN_ISSUERID"].isna(),
                        self.datasource.df["ALADDIN_ISSUERID"],
                        self.datasource.df["ISIN"],
                    ),
                ),
            ),
        )

        replace_nas = [
            "BCLASS_LEVEL2",
            "BCLASS_LEVEL3",
            "BCLASS_LEVEL4",
            "GICS_LEVEL2",
            "GICS_LEVEL3",
            "GICS_LEVEL4",
            "JPM_SECTOR",
        ]
        for col in replace_nas:
            replace_df = self.datasource.df[["ISSUER_ID", col]]
            replace_df = replace_df.dropna(subset=col)
            replace_df = dict(zip(replace_df["ISSUER_ID"], replace_df[col]))
            self.datasource.df[col] = self.datasource.df[col].fillna(
                self.datasource.df["ISSUER_ID"].map(replace_df)
            )

        self.datasource.df[
            [
                "BCLASS_LEVEL2",
                "BCLASS_LEVEL3",
                "BCLASS_LEVEL4",
                "GICS_LEVEL2",
                "GICS_LEVEL3",
                "GICS_LEVEL4",
            ]
        ] = self.datasource.df[
            [
                "BCLASS_LEVEL2",
                "BCLASS_LEVEL3",
                "BCLASS_LEVEL4",
                "GICS_LEVEL2",
                "GICS_LEVEL3",
                "GICS_LEVEL4",
            ]
        ].apply(
            lambda col: col.str.title()
        )

        self.datasource.df[
            [
                "BCLASS_LEVEL2",
                "BCLASS_LEVEL3",
                "BCLASS_LEVEL4",
            ]
        ] = self.datasource.df[
            [
                "BCLASS_LEVEL2",
                "BCLASS_LEVEL3",
                "BCLASS_LEVEL4",
            ]
        ].apply(
            lambda col: col.fillna("Unassigned BCLASS")
        )

        self.datasource.df[
            ["GICS_LEVEL2", "GICS_LEVEL3", "GICS_LEVEL4"]
        ] = self.datasource.df[["GICS_LEVEL2", "GICS_LEVEL3", "GICS_LEVEL4"]].apply(
            lambda col: col.fillna("Unassigned GICS")
        )

        self.datasource.df[
            ["MSCI_ISSUERID", "BBG_ISSUERID", "ISS_ISSUERID", "ALADDIN_ISSUERID"]
        ] = self.datasource.df[
            ["MSCI_ISSUERID", "BBG_ISSUERID", "ISS_ISSUERID", "ALADDIN_ISSUERID"]
        ].apply(
            lambda col: col.fillna("NoISSUERID")
        )

        df_equity = self.datasource.df[self.datasource.df["SECURITY_TYPE"] == "Equity"][
            ["ISSUER_ID", "SECURITY_NAME"]
        ]
        df_equity = df_equity.rename(columns={"SECURITY_NAME": "COMPANY_NAME"})
        self.datasource.df = self.datasource.df.merge(
            right=df_equity, left_on="ISSUER_ID", right_on="ISSUER_ID", how="left"
        )
        self.datasource.df["COMPANY_NAME"] = self.datasource.df["COMPANY_NAME"].fillna(
            self.datasource.df.groupby(["ISSUER_ID"])["SECURITY_NAME"].transform(
                lambda x: pd.Series.mode(x)[0]
            )
        )

    def iter(
        self,
    ) -> None:
        """
        Iterate over portfolio holdings
        - Create Security objects
        - create Company, Muni, Sovereign, Securitized, Cash objects
        """
        self.msci_ids = list(self.df["MSCI_ISSUERID"].unique())
        self.bbg_ids = list(self.df["BBG_ISSUERID"].unique())

        sec_df = self.df[
            [
                "DATE",
                "SECURITY_KEY",
                "ISIN",
                "CUSIP",
                "TICKER",
                "SECURITY_NAME",
                "SECURITY_TYPE",
                "SECTOR_LEVEL_1",
                "SECTOR_LEVEL_2",
                "LOAN_CATEGORY",
                "LABELED_ESG_TYPE",
                "ISSUER_ESG",
                "TCW_ESG",
                "ESG_COLLATERAL_TYPE",
                "COUNTRY_OF_RISK",
                "ISO2",
                "ISSUER_ID",
            ]
        ]
        comp_df = self.df[
            [
                "DATE",
                "ISSUER_ID",
                "MSCI_ISSUERID",
                "BBG_ISSUERID",
                "ISS_ISSUERID",
                "ALADDIN_ISSUERID",
                "JPM_SECTOR",
                "BCLASS_LEVEL2",
                "BCLASS_LEVEL3",
                "BCLASS_LEVEL4",
                "GICS_LEVEL2",
                "GICS_LEVEL3",
                "GICS_LEVEL4",
                "COMPANY_NAME",
            ]
        ]
        comp_df = comp_df.drop_duplicates(subset="ISSUER_ID")

        for index, row in comp_df.iterrows():
            comp_info = row.to_dict()
            self.create_issuer_store(comp_info)

        for index, row in sec_df.iterrows():
            sec_info = row.to_dict()
            self.create_security_store(sec_info)

    def create_security_store(self, security_information: dict) -> None:
        """
        create security store

        Parameters
        ----------
        security_information: dict
            dictionary of information about the security
        """
        sec_key = security_information["SECURITY_KEY"]
        security_store = securities.SecurityStore(
            key=sec_key, information=security_information
        )
        self.securities[sec_key] = security_store
        issuer_store = self.issuers[security_information["ISSUER_ID"]]
        security_store.add_issuer(issuer_store)
        issuer_store.add_security(sec_key, security_store)

    def create_issuer_store(self, issuer_information: dict) -> None:
        """
        create issuer store

        Parameters
        ----------
        issuer_information: dict
            dictionary of information about the issuer
        """
        issuer_key = issuer_information["ISSUER_ID"]
        issuer_store = issuer.IssuerStore(
            key=issuer_key, information=issuer_information
        )
        self.issuers[issuer_key] = issuer_store

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.datasource.df
