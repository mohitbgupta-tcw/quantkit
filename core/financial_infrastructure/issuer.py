class IssuerStore(object):
    """
    Issuer object. Basket for Securities
    Stores information such as:
        - id
        - attached securities (Equity and Bonds)
        - industry (including GICS and BCLASS objects)
        - scores

    Parameters
    ----------
    key: str
        issuer's key
    information: dict
        dictionary of issuer specific information
    """

    def __init__(self, key: str, information: dict, **kwargs) -> None:
        self.key = key
        self.securities = dict()
        self.munis = dict()
        self.securitized = dict()
        self.corporates = dict()
        self.cash = dict()
        self.sovereigns = dict()
        self.information = information

    def add_security(
        self,
        key: str,
        store,
    ) -> None:
        """
        Add security object to parent.
        Security could be stock or issued Fixed Income of company.

        Parameters
        ----------
        key: str
            security's key
        store: SecurityStore
            security store of new security
        """
        self.securities[key] = store

    def remove_security(self, key: str) -> None:
        """
        Remove security object from company.

        Parameters
        ----------
        key: str
            security's key
        """
        self.securities.pop(key, None)
