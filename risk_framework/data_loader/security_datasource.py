import quantkit.risk_framework.financial_infrastructure.securities as securities
import quantkit.risk_framework.financial_infrastructure.issuer as issuer
from quantkit.core.data_loader.security_datasource import SecurityDataSource


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

    def create_sectorlevel2_store(self, msci_dict: dict) -> None:
        """
        Iterate over portfolio holdings
        - Create Security objects
        - create Company, Muni, Sovereign, Securitized, Cash objects
        - attach holdings, OAS to self.holdings with security object

        Parameters
        ----------
        msci_dict: dict
            dictionary of msci information
        """
        security_type_mapping = {
            "Securitized": securitized.SecuritizedStore,
            "Muni": munis.MuniStore,
            "Sovereign": sovereigns.SovereignStore,
            "Cash": cash.CashStore,
            "Corporate": comp.CompanyStore,
        }

        return super().create_sectorlevel2_store(msci_dict, security_type_mapping)
