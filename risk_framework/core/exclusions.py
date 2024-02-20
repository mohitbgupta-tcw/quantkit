import numpy as np


class Exclusion(object):
    """
    Main Class for Exclusions logic
    """

    def __init__(self):
        self.exclusion_dict = dict()

    def contrweap_tie(self, cweap_tie: bool) -> None:
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

    def weapons_revenue(self, weap_max_rev_pct: float, threshold: float) -> None:
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

    def firearm_revenue(self, firearm_max_rev_pct: float, threshold: float) -> None:
        """
        The recent-year percentage of revenue, or maximum estimated percent,
        a company has derived from the manufacture and retail of
        civilian firearms and ammunition.

        Parameters
        ----------
        firearm_max_rev_pct: float
            Firearm revenue percentage
        threshold: float
            revenue threshold
        """
        if firearm_max_rev_pct > threshold:
            self.exclusion_dict["FIREARM_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["FIREARM_MAX_REV_PCT"] = False

    def tobacco_revenue(self, tob_max_rev_pct: float, threshold: float) -> None:
        """
        The recent-year percent of revenue, or maximum estimated percent,
        a company has derived from tobacco-related business activities.

        Parameters
        ----------
        tob_max_rev_pct: float
            Tobacco revenue percentage
        threshold: float
            revenue threshold
        """
        if tob_max_rev_pct > threshold:
            self.exclusion_dict["TOB_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["TOB_MAX_REV_PCT"] = False

    def thermal_coal_revenue(
        self, thermal_coal_max_rev_pct: float, threshold: float
    ) -> None:
        """
        This factor identifies the maximum percentage of revenue
        (either reported or estimated) greater than 0% that a company derives from
        the mining of thermal coal (including lignite, bituminous, anthracite and
        steam coal) and its sale to external parties. It excludes: revenue from
        metallurgical coal; coal mined for internal power generation
        (e.g. in the case of vertically integrated power producers);
        intra-company sales of mined thermal coal; and revenue from coal trading.

        Parameters
        ----------
        thermal_coal_max_rev_pct: float
            Thermal Coal revenue percentage
        threshold: float
            revenue threshold
        """
        if thermal_coal_max_rev_pct > threshold:
            self.exclusion_dict["THERMAL_COAL_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["THERMAL_COAL_MAX_REV_PCT"] = False

    def unconventional_oil_gas_revenue(
        self, unconv_oil_gas_max_rev_pct: float, threshold: float
    ) -> None:
        """
        This factor identifies the maximum percentage of revenue
        (either reported or estimated) greater than 0% that a company derives
        from unconventional oil and gas. It includes revenues from oil sands,
        oil shale (kerogen-rich deposits), shale gas, shale oil, coal seam gas,
        and coal bed methane.

        Parameters
        ----------
        unconv_oil_gas_max_rev_pct: float
            Unconventional Oil and Gas revenue percentage
        threshold: float
            revenue threshold
        """
        if unconv_oil_gas_max_rev_pct > threshold:
            self.exclusion_dict["UNCONV_OIL_GAS_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["UNCONV_OIL_GAS_MAX_REV_PCT"] = False

    def oil_gas_revenue(self, og_rev: float, threshold: float) -> None:
        """
        This factor identifies the maximum percentage of revenue
        (either reported or estimated) that a company derives from oil and gas
        related activities, including distribution / retail, equipment and services,
        extraction and production, petrochemicals, pipelines and transportation
        and refining but excluding biofuel production and sales and trading activities.

        Parameters
        ----------
        og_rev: float
            Oil and Gas revenue percentage
        threshold: float
            revenue threshold
        """
        if og_rev > threshold:
            self.exclusion_dict["OG_REV"] = True
        else:
            self.exclusion_dict["OG_REV"] = False

    def thermal_coal_power_generation_revenue(
        self, generat_max_rev_thermal_coal: float, threshold: float
    ) -> None:
        """
        This factor identifies the maximum percentage of revenue
        (either reported or estimated) that a company derives from the
        thermal coal based power generation.

        Parameters
        ----------
        generat_max_rev_thermal_coal: float
            Power Generation from Thermal Coal revenue percentage
        threshold: float
            revenue threshold
        """
        if generat_max_rev_thermal_coal > threshold:
            self.exclusion_dict["GENERAT_MAX_REV_THERMAL_COAL"] = True
        else:
            self.exclusion_dict["GENERAT_MAX_REV_THERMAL_COAL"] = False

    def adult_entertainment_revenue(
        self, ae_max_rev_pct: float, threshold: float
    ) -> None:
        """
        The recent-year percent of revenue, or maximum estimated percent,
        a company has derived from adult entertainment.

        Parameters
        ----------
        ae_max_rev_pct: float
            Adult Entertainment revenue percentage
        threshold: float
            revenue threshold
        """
        if ae_max_rev_pct > threshold:
            self.exclusion_dict["AE_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["AE_MAX_REV_PCT"] = False

    def alcohol_revenue(
        self, alc_prod_max_rev_pct: float, alc_dist_max_rev_pct: float, threshold: float
    ) -> None:
        """
        The recent-year percent of revenue, or maximum estimated percent,
        a company has derived from manufacture of alcoholic products.
        AND The recent-year percent of revenue, or maximum estimated percent,
        a company has derived as a Distributor of alcohol products.

        Parameters
        ----------
        alc_prod_max_rev_pct: float
            Alcohol Production revenue percentage
        alc_dist_max_rev_pct: float
            Alcohol Distribution revenue percentage
        threshold: float
            revenue threshold
        """
        alcohol_rev = np.nansum([alc_dist_max_rev_pct, alc_prod_max_rev_pct])
        if alcohol_rev > threshold:
            self.exclusion_dict["ALC_PROD_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["ALC_PROD_MAX_REV_PCT"] = False

    def gambling_revenue(self, gam_max_rev_pct: float, threshold: float) -> None:
        """
        The recent-year percent of revenue, or maximum estimated percent,
        a company has derived from gambling-related business activities.

        Parameters
        ----------
        gam_max_rev_pct: float
            Gambling revenue percentage
        threshold: float
            revenue threshold
        """
        if gam_max_rev_pct > threshold:
            self.exclusion_dict["GAM_MAX_REV_PCT"] = True
        else:
            self.exclusion_dict["GAM_MAX_REV_PCT"] = False

    def compliance_ungc(self, ungc_compliance: str) -> None:
        """
        This factor indicates whether the company is in compliance with the
        United Nations Global Compact principles.
        The possible values are Fail, Watch List, or Pass.
        See the ESG Controversies and Global Norms methodology document for
        detailed explanations.

        Parameters
        ----------
        ungc_compliance: str
            Issuer not in compliance with the United Nations Global Compact
        """
        if ungc_compliance == "Fail":
            self.exclusion_dict["UNGC_COMPLIANCE"] = True
        else:
            self.exclusion_dict["UNGC_COMPLIANCE"] = False

    def compliance_hr(self, hr_compliance: str) -> None:
        """
        This factor indicates whether the company is in compliance with the
        United Nations Guiding Principles for Business and Human Rights.
        The possible values are Fail, Watch List, or Pass. See the ESG
        Controversies and Global Norms methodology document for detailed explanations.

        Parameters
        ----------
        hr_compliance: str
            Issuer not in compliance with United Nations Guiding Principles
            for Business and Human Rights
        """
        if hr_compliance == "Fail":
            self.exclusion_dict["HR_COMPLIANCE"] = True
        else:
            self.exclusion_dict["HR_COMPLIANCE"] = False

    def msci_esg_rating(self, iva_company_rating: str) -> None:
        """
        A company's final ESG Rating. To arrive at a final letter rating,
        the weighted average of the key issue scores are aggregated and
        companies are ranked from best (AAA) to worst (CCC).

        Parameters
        ----------
        iva_company_rating: str
            Issuer's MSCI IVA Company Rating
        """
        if iva_company_rating == "CCC":
            self.exclusion_dict["IVA_COMPANY_RATING"] = True
        else:
            self.exclusion_dict["IVA_COMPANY_RATING"] = False


class A8(Exclusion):
    """
    Article 8 Exclusions

    Parameters
    ----------
    cweap_tie: bool
        Issuers with ties to Controversial Weapons
    weap_max_rev_pct: float
        Weapons revenue percentage
    firearm_max_rev_pct: float
        Firearm revenue percentage
    tob_max_rev_pct: float
        Tobacco revenue percentage
    thermal_coal_max_rev_pct: float
        Thermal Coal revenue percentage
    generat_max_rev_thermal_coal: float
        Power Generation from Thermal Coal revenue percentage
    ungc_compliance: str
        Issuer not in compliance with the United Nations Global Compact
    iva_company_rating: str
        Issuer's MSCI IVA Company Rating
    """

    def __init__(
        self,
        cweap_tie: bool,
        weap_max_rev_pct: float,
        firearm_max_rev_pct: float,
        tob_max_rev_pct: float,
        thermal_coal_max_rev_pct: float,
        unconv_oil_gas_max_rev_pct: float,
        generat_max_rev_thermal_coal: float,
        ungc_compliance: str,
        iva_company_rating: str,
    ):
        super().__init__()
        self.contrweap_tie(cweap_tie)
        self.weapons_revenue(weap_max_rev_pct, threshold=5)
        self.firearm_revenue(firearm_max_rev_pct, threshold=5)
        self.tobacco_revenue(tob_max_rev_pct, threshold=5)
        self.thermal_coal_revenue(thermal_coal_max_rev_pct, threshold=5)
        self.unconventional_oil_gas_revenue(unconv_oil_gas_max_rev_pct, threshold=5)
        self.thermal_coal_power_generation_revenue(
            generat_max_rev_thermal_coal, threshold=30
        )
        self.compliance_ungc(ungc_compliance)
        self.msci_esg_rating(iva_company_rating)


class A9(Exclusion):
    """
    Article 9 Exclusions

    Parameters
    ----------
    cweap_tie: bool
        Issuers with ties to Controversial Weapons
    weap_max_rev_pct: float
        Weapons revenue percentage
    firearm_max_rev_pct: float
        Firearm revenue percentage
    tob_max_rev_pct: float
        Tobacco revenue percentage
    thermal_coal_max_rev_pct: float
        Thermal Coal revenue percentage
    og_rev: float
        Oil and Gas revenue percentage
    generat_max_rev_thermal_coal: float
        Power Generation from Thermal Coal revenue percentage
    ae_max_rev_pct: float
        Adult Entertainment revenue percentage
    alc_prod_max_rev_pct: float
        Alcohol Production revenue percentage
    alc_dist_max_rev_pct: float
        Alcohol Distribution revenue percentage
    gam_max_rev_pct: float
        Gambling revenue percentage
    ungc_compliance: str
        Issuer not in compliance with the United Nations Global Compact
    hr_compliance: str
        Issuer not in compliance with United Nations Guiding Principles
        for Business and Human Rights
    iva_company_rating: str
        Issuer's MSCI IVA Company Rating
    """

    def __init__(
        self,
        cweap_tie: bool,
        weap_max_rev_pct: float,
        firearm_max_rev_pct: float,
        tob_max_rev_pct: float,
        thermal_coal_max_rev_pct: float,
        unconv_oil_gas_max_rev_pct: float,
        og_rev: float,
        generat_max_rev_thermal_coal: float,
        ae_max_rev_pct: float,
        alc_prod_max_rev_pct: float,
        alc_dist_max_rev_pct: float,
        gam_max_rev_pct: float,
        ungc_compliance: str,
        hr_compliance: str,
        iva_company_rating: str,
    ):
        super().__init__()
        self.contrweap_tie(cweap_tie)
        self.weapons_revenue(weap_max_rev_pct, threshold=5)
        self.firearm_revenue(firearm_max_rev_pct, threshold=5)
        self.tobacco_revenue(tob_max_rev_pct, threshold=5)
        self.thermal_coal_revenue(thermal_coal_max_rev_pct, threshold=0)
        self.unconventional_oil_gas_revenue(unconv_oil_gas_max_rev_pct, threshold=5)
        self.oil_gas_revenue(og_rev, threshold=5)
        self.thermal_coal_power_generation_revenue(
            generat_max_rev_thermal_coal, threshold=10
        )
        self.adult_entertainment_revenue(ae_max_rev_pct, threshold=5)
        self.alcohol_revenue(alc_prod_max_rev_pct, alc_dist_max_rev_pct, threshold=5)
        self.gambling_revenue(gam_max_rev_pct, threshold=5)
        self.compliance_ungc(ungc_compliance)
        self.compliance_hr(hr_compliance)
        self.msci_esg_rating(iva_company_rating)
