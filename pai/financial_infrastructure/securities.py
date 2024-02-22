import quantkit.core.financial_infrastructure.securities as securities


class SecurityStore(securities.SecurityStore):
    """
    Security Object
    Stores information such as:
        - isin
        - parent (as store)
        - portfolios the security is held in (as store)
        - ESG factors
        - security information

    Parameters
    ----------
    isin: str
        Security's isin
    information: dict
        dictionary of security specific information
    """

    def __init__(self, isin: str, information: dict, **kwargs) -> None:
        super().__init__(isin, information, **kwargs)

    def iter(self, **kwargs) -> None:
        """
        Iter over security and attach information
        """
        super().iter(**kwargs)
