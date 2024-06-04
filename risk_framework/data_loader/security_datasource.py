import quantkit.risk_framework.financial_infrastructure.securities as securities
import quantkit.risk_framework.financial_infrastructure.issuer as issuer
from quantkit.core.data_loader.security_datasource import SecurityDataSource
from quantkit.risk_framework.characteristics.sector_level2 import (
    MuniStore,
    SecuritizedStore,
    CorporateStore,
    SovereignStore,
    CashStore,
)


class SecurityDataSource(SecurityDataSource):
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
        self.create_sectorlevel2_store(security_store, issuer_store)

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

    def create_sectorlevel2_store(
        self,
        security_store: securities.SecurityStore,
        issuer_store: issuer.IssuerStore,
    ) -> None:
        """
        create new objects for Sector Level 2

        Parameters
        ----------
        security_store: SecurityStore
            security store
        issuer_store: IssuerStore
            issuer store
        """
        sec_id = security_store.information["SECURITY_KEY"]
        sector_level_2 = security_store.information["SECTOR_LEVEL_2"]

        if sector_level_2 in ["Muni / Local Authority"]:
            s2_store = MuniStore(security_store, sector_level_2)
            issuer_store.munis[sec_id] = security_store
        elif sector_level_2 in ["Residential MBS", "CMBS", "ABS"]:
            s2_store = SecuritizedStore(security_store, sector_level_2)
            issuer_store.securitized[sec_id] = security_store
        elif sector_level_2 in ["Sovereign"]:
            s2_store = SovereignStore(security_store, sector_level_2)
            issuer_store.sovereigns[sec_id] = security_store
        elif sector_level_2 in ["Cash and Other"]:
            s2_store = CashStore(security_store, sector_level_2)
            issuer_store.cash[sec_id] = security_store
        else:
            s2_store = CorporateStore(security_store, sector_level_2)
            issuer_store.corporates[sec_id] = security_store

        security_store.information["SECTOR_LEVEL_2"] = s2_store
