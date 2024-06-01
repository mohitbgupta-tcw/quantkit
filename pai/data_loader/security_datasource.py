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

    pass
