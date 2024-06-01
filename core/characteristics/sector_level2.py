class SectorLevel2Store(object):
    """
    Sector Level 2 Main Store

    Parameters
    ----------
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, sector_level2: str, **kwargs) -> None:
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
    Corporate Sector object

    Parameters
    ----------
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, sector_level2: str, **kwargs) -> None:
        super().__init__(sector_level2, **kwargs)
        self.sector_level1 = "corporate"


class MuniStore(SectorLevel2Store):
    """
    Muni Sector object

    Parameters
    ----------
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, sector_level2: str, **kwargs) -> None:
        super().__init__(sector_level2, **kwargs)
        self.sector_level1 = "muni"


class SecuritizedStore(SectorLevel2Store):
    """
    Securitized Sector object

    Parameters
    ----------
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, sector_level2: str, **kwargs) -> None:
        super().__init__(sector_level2, **kwargs)
        self.sector_level1 = "securitized"


class CashStore(SectorLevel2Store):
    """
    Cash Sector object

    Parameters
    ----------
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, sector_level2: str, **kwargs) -> None:
        super().__init__(sector_level2, **kwargs)
        self.sector_level1 = "cash"


class SovereignStore(SectorLevel2Store):
    """
    Sovereign Sector object

    Parameters
    ----------
    sector_level2: str
        sector level 2 label
    """

    def __init__(self, sector_level2: str, **kwargs) -> None:
        super().__init__(sector_level2, **kwargs)
        self.sector_level1 = "sovereign"
