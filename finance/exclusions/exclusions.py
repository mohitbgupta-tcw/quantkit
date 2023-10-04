class Exclusion(object):
    """ """

    def __init__(self):
        self.exclusion_dict = dict()

    def contrweap_tie(self, cweap_tie: bool):
        """
        Companies that have any ties to cluster munitions,
        landmines, biological / chemical weapons, depleted uranium weapons,
        blinding laser weapons, incendiary weapons, and/or non-detectable fragments.

        Parameters
        ----------
        cweap_tie: bool
            Issuers with ties to Controversial Weapons
        """
        if cweap_tie == True:
            self.exclusion_dict["CWEAP_TIE"] = True
        else:
            self.exclusion_dict["CWEAP_TIE"] = False

    def weapons_revenue(self, weap_max_rev_pct: float, threshold: float):
        """
        The recent-year percent of revenue, or maximum estimated percent,
        a company has derived from weapons systems, components,
        and support systems and services.

        Parameters
        ----------
        weap_max_rev_pct: float
            Weapons revenue percentage
        threshold: float
            revenue threshold
        """
        if weap_max_rev_pct > threshold:
            self.exclusion_dict["WEAP_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["WEAP_MAX_REV_PCT"] = False


class A8(Exclusion):
    """

    Parameters
    ----------
    cweap_tie: bool
        Issuers with ties to Controversial Weapons
    weap_max_rev_pct: float
        Weapons revenue percentage
    """

    def __init__(self, cweap_tie: bool, weap_max_rev_pct: float):
        super().__init__()
        self.contrweap_tie(cweap_tie)
        self.weapons_revenue(weap_max_rev_pct, threshold=5)


class A9(Exclusion):
    """

    Parameters
    ----------
    cweap_tie: bool
        Issuers with ties to Controversial Weapons
    weap_max_rev_pct: float
        Weapons revenue percentage
    """

    def __init__(self, cweap_tie: bool, weap_max_rev_pct: float):
        super().__init__()
        self.contrweap_tie(cweap_tie)
        self.weapons_revenue(weap_max_rev_pct, threshold=5)
