class SectorLevel2Store(object):
    """
    Sector Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, security, sector_level2: str, **kwargs) -> None:
        self.security = security
        self.sector_level2 = sector_level2

    def iter(
        self,
        **kwargs,
    ) -> None:
        """
        Iterate over sector object
        """
        pass


class CorporateStore(SectorLevel2Store):
    """
    Corporate Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, security, sector_level2: str, **kwargs) -> None:
        super().__init__(security, sector_level2, **kwargs)
        self.sector_level1 = "corporate"


class MuniStore(SectorLevel2Store):
    """
    Muni Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, security, sector_level2: str, **kwargs) -> None:
        super().__init__(security, sector_level2, **kwargs)
        self.sector_level1 = "muni"


class SecuritizedStore(SectorLevel2Store):
    """
    Muni Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, security, sector_level2: str, **kwargs) -> None:
        super().__init__(security, sector_level2, **kwargs)
        self.sector_level1 = "securitized"


class CashStore(SectorLevel2Store):
    """
    Cash Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, security, sector_level2: str, **kwargs) -> None:
        super().__init__(security, sector_level2, **kwargs)
        self.sector_level1 = "cash"


class SovereignStore(SectorLevel2Store):
    """
    Sovereign Level 2 Main Store

    Parameters
    ----------
    security: SecurityStore
        underlying security
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, security, sector_level2: str, **kwargs) -> None:
        super().__init__(security, sector_level2, **kwargs)
        self.sector_level1 = "sovereign"
