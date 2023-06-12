import pandas as pd
from typing import Union


class Theme(object):
    """
    Sustainability Theme Object
    Stores information such as:
        - acronym (one word description of theme)
        - name (description of theme)
        - pillar (sustainability classification, either People or Planet)
        - iss_cols (all ISS datafields used for theme calculation)
        - msci_sub (all MSCI datafields used for theme calculation)
        - product_key_add (words specific to theme)
             - if word is in description of company, it is likely that company belongs to theme
        - params (parameters for theme calculations such as revenue thresholds, industry exclusions and inclusions)
        - companies attached to theme (as store)

    Parameters
    ----------
    acronym: str
        Theme identifier (one word description)
    name: str
        Theme name (description)
    pillar: str
        sustainability main category ('Planet' or 'People')
    information_df: pd.DataFrame
        DataFrame with all information about theme
    params: dict
        parameters for theme calculations (stored in \\quantkit\\utils\\configs.json)
    """

    def __init__(
        self,
        acronym: str,
        name: str,
        pillar: str,
        information_df: pd.DataFrame,
        params: dict,
    ):
        self.acronym = acronym
        self.name = name
        self.pillar = pillar
        self.information_df = information_df
        self.iss_cols = self.add_ISS()
        self.msci_sub = self.add_MSCI_sub()
        self.product_key_add = self.add_product_key()
        self.params = params
        self.companies = dict()

    def add_ISS(self):
        """
        Add data fields from ISS. These data fields are used to check if company should be
        assigned to this theme.

        Returns
        --------
        list
            list of theme specific columns provided by ISS
        """
        ISS1Uniques = set(self.information_df["ISS 1"].unique())
        ISS2Uniques = set(self.information_df["ISS 2"].unique())
        iss_list = ISS1Uniques.union(ISS2Uniques)
        iss_cols = []
        for iss in iss_list:
            if not (pd.isna(iss) or iss in iss_cols):
                iss_cols.append(iss)
        return iss_cols

    def add_MSCI_sub(self):
        """
        Add data fields from MSCI. These data fields are used to check if company should be
        assigned to this theme.

        Returns
        -------
        list
            list of theme specific columns provided by MSCI
        """
        msci_list = set(self.information_df["MSCI Subcategories"].unique())
        msci_sub = []
        for sub in msci_list:
            if not (pd.isna(sub) or sub in msci_sub):
                msci_sub.append(sub)
        return msci_sub

    def add_product_key(self):
        """
        Add theme specific words. When these words occur in company's description,
        it is more likely that this company belongs to theme

        Returns
        -------
        list
            list of theme specific words
        """
        product_list = set(self.information_df["ProductKeyAdd"].unique())
        product_key_add = []
        for p in product_list:
            if not (pd.isna(p) or p in product_key_add):
                product_key_add.append(p)
        return product_key_add

    def exclude_rule(self, industry: str, exclusions: list, **kwargs):
        """
        Checks if a company's industry is in exclusion list
        (list of industries company's industry should not be in to be included in theme)
        if industry is excluded, return False, else True

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        exclusions: list
            list of industries for exclusion

        Returns
        -------
        bool
            industry excluded
        """
        if industry in exclusions:
            return False
        return True

    def include_rule(self, industry: str, inclusions: list, **kwargs):
        """
        Checks if a company's industry is in inclusion list
        (list of industries company's industry should be in to be included in theme)

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        exclusions: list
            list of industries for inclusion

        Returns
        -------
        bool
            industry included
        """
        if industry in inclusions:
            return True
        return False

    def bool_rule(self, b: bool, **kwargs):
        """
        Check if boolean is True.

        Parameters
        ----------
        b: bool
            boolean to be checked

        Returns
        -------
        bool
            input is True
        """
        if b == True:
            return True
        return False

    def reverse_bool_rule(self, b: bool, **kwargs):
        """
        Reverse a boolean: if True, return False. If False or nan, return True

        Parameters
        ----------
        b: bool
            boolean to be checked

        Returns
        -------
        bool
            Input is False
        """
        if b == True:
            return False
        return True

    def bigger_eq_rule(self, val: float, threshold: float, **kwargs):
        """
        Check if value inputted is bigger or equal than specified threshold

        Parameters
        ----------
        val: float
            value
        threshold: float
            threshold value

        Returns
        -------
        bool
            input is bigger than threshold
        """
        if val >= threshold:
            return True
        return False

    def exclusion_key_rule(
        self, industry: str, exclusions: list, iss_key: bool, **kwargs
    ):
        """
        Two way rule: either industry is not in exclusions or if industry is in exclusions,
        check if company has keyword for theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        exclusions: list
            list of industries for exclusion
        iss_key: bool
            boolean: company has theme specific word in description

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(industry, exclusions) or self.bool_rule(iss_key):
            return True
        return False

    def inclusion_key_rule(
        self, industry: str, inclusions: list, iss_key: bool, **kwargs
    ):
        """
        Two way rule: either industry is in inclusions or check if company has keyword for theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        inclusions: list
            list of industries for inclusion
        iss_key: bool
            boolean: company has theme specific word in description

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.include_rule(industry, inclusions) or self.bool_rule(iss_key):
            return True
        return False

    def revenue_bigger_rule(self, rev: float, min_revenue: float, **kwargs):
        """
        Check if revenue inputted is bigger or equal than specified minimum value

        Parameters
        ----------
        rev: float
            revenue
        min_revenue: float
            minimum revenue threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        return self.bigger_eq_rule(rev, min_revenue)

    def min_revenue_rule(
        self, msci_rev: float, iss_rev: float, revenue_threshold: float, **kwargs
    ):
        """
        Check if either revenue calculated on MSCI or ISS measures is bigger than threshold

        Parameters
        ----------
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        revenue_threshold: float
            minimum revenue threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.revenue_bigger_rule(
            msci_rev, revenue_threshold
        ) or self.revenue_bigger_rule(iss_rev, revenue_threshold):
            return True
        return False

    def CapEx_rule(self, capex: float, capex_threshold: float, **kwargs):
        """
        Check if capex inputted is bigger or equal than specified minimum value

        Parameters
        ----------
        capex: float
            capex
        capex_threshold: float
            minimum capex threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        return self.bigger_eq_rule(capex, capex_threshold)

    def revenue_capex_rule(
        self,
        msci_rev: float,
        iss_rev: float,
        min_revenue: float,
        capex: float,
        capex_threshold: float,
        **kwargs
    ):
        """
        Check if MSCI or ISS revenue is bigger than revenue threshold
        AND
        capex is bigger than capex threshold

        Parameters
        ----------
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        min_revenue: float
            minimum revenue threshold
        capex: float
            capex
        capex_threshold: float
            minimum capex threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.min_revenue_rule(msci_rev, iss_rev, min_revenue) and self.CapEx_rule(
            capex, capex_threshold
        ):
            return True
        return False

    def revenue_rule(
        self,
        msci_rev: float,
        iss_rev: float,
        revenue_threshold: float,
        min_revenue: float,
        capex: float,
        capex_threshold: float,
        **kwargs
    ):
        """
        Check if MSCI or ISS revenue is bigger than revenue threshold
        OR
        check if revenue is bigger than smaller threshold but capex in sustainability
        is bigger than capex threshold at the same time

        Parameters
        ----------
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        revenue_threshold: float
            minimum revenue threshold
        min_revenue: float
            minimum revenue threshold if capex rule is also fulfilled
        capex: float
            capex
        capex_threshold: float
            minimum capex threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.min_revenue_rule(
            msci_rev, iss_rev, revenue_threshold
        ) or self.revenue_capex_rule(
            msci_rev, iss_rev, min_revenue, capex, capex_threshold
        ):
            return True
        return False

    def ISS_inclusion_key_rule(
        self,
        inclusions: list,
        industry: str,
        iss_key: bool,
        iss_rev: float,
        revenue_threshold: float,
        **kwargs
    ):
        """
        Check if ISS revenue is bigger than revenue threshold
        AND
        either industry is in inclusions or company has keyword for theme in description

        Parameters
        ----------
        inclusions: list
            list of industries for inclusion
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        iss_rev: float
            revenue calculated on ISS measures
        revenue_threshold: float
            minimum revenue threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.revenue_bigger_rule(
            iss_rev, revenue_threshold
        ) and self.inclusion_key_rule(industry, inclusions, iss_key):
            return True
        return False

    def MSCI_inclusion_rule(
        self,
        inclusions: list,
        industry: str,
        msci_rev: float,
        revenue_threshold: float,
        **kwargs
    ):
        """
        Check if MSCI revenue is bigger than revenue threshold
        AND
        industry is in inclusions

        Parameters
        ----------
        inclusions: list
            list of industries for inclusion
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        revenue_threshold: float
            minimum revenue threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.revenue_bigger_rule(msci_rev, revenue_threshold) and self.include_rule(
            industry, inclusions
        ):
            return True
        return False

    def MSCI_ISS_inclusion_key_rule(
        self,
        inclusions: list,
        industry: str,
        iss_key: bool,
        msci_rev: float,
        iss_rev: float,
        revenue_threshold: float,
        **kwargs
    ):
        """
        Check if MSCI revenue is bigger than revenue threshold
        OR
        (
            Check if ISS revenue is bigger than revenue threshold
            AND
            either industry is in inclusions or company has keyword for theme in description
        )

        Parameters
        ----------
        inclusions: list
            list of industries for inclusion
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        revenue_threshold: float
            minimum revenue threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.revenue_bigger_rule(
            msci_rev, revenue_threshold
        ) or self.ISS_inclusion_key_rule(
            inclusions, industry, iss_key, iss_rev, revenue_threshold
        ):
            return True
        return False

    def MSCI_ISS_inclusion_key_capex_rule(
        self,
        inclusions: list,
        industry: str,
        iss_key: bool,
        msci_rev: float,
        iss_rev: float,
        revenue_threshold: float,
        min_revenue: float,
        capex: float,
        capex_threshold: float,
        **kwargs
    ):
        """
        (
            Check if MSCI revenue is bigger than revenue threshold
            OR
            (
                Check if ISS revenue is bigger than revenue threshold
                AND
                either industry is in inclusions or company has keyword for theme in description
            )
        )
        OR
        (
            Check if MSCI or ISS revenue is bigger than minimum revenue threshold
            AND
            capex is bigger than capex threshold
        )

        Parameters
        ----------
        inclusions: list
            list of industries for inclusion
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        revenue_threshold: float
            minimum revenue threshold
        min_revenue: float
            minimum revenue threshold if capex rule is also fulfilled
        capex: float
            capex
        capex_threshold: float
            minimum capex threshold

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.MSCI_ISS_inclusion_key_rule(
            inclusions, industry, iss_key, msci_rev, iss_rev, revenue_threshold
        ) or self.revenue_capex_rule(
            msci_rev, iss_rev, min_revenue, capex, capex_threshold
        ):
            return True
        return False

    def social_impact_revenue_inclusion_rule(
        self,
        industry: str,
        inclusions: list,
        social_inclusion: float,
        si_threshold: float,
        **kwargs
    ):
        """
        Checks if a company's industry is in inclusion list
        AND
        revenue derived from any basic needs social impact themes is bigger than threshold

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        inclusions: list
            list of industries for inclusion
        social_inclusion: float
            total of all revenues derived from any of the basic needs social impact themes
        si_threshold: float
            min revenue threshold for social impact themes

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.include_rule(industry, inclusions) and self.bigger_eq_rule(
            social_inclusion, si_threshold
        ):
            return True
        return False

    def MSCI_social_impact_revenue_inclusion_rule(
        self,
        msci_rev: float,
        revenue_threshold: float,
        industry: str,
        inclusions: list,
        social_inclusion: float,
        si_threshold: float,
        **kwargs
    ):
        """
        Check is MSCI revenue is bigger than minimum revenue threshold
        OR
        (
            a company's industry is in inclusion list
            AND
            revenue derived from any basic needs social impact themes is bigger than threshold
        )

        Parameters
        ----------
        msci_rev: float
            revenue calculated on MSCI measures
        revenue_threshold: float
            minimum revenue threshold
        industry: str
            name of industry company belongs to
        inclusions: list
            list of industries for inclusion
        social_inclusion: float
            total of all revenues derived from any of the basic needs social impact themes
        si_threshold: float
            min revenue threshold for social impact themes

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.revenue_bigger_rule(
            msci_rev, revenue_threshold
        ) or self.social_impact_revenue_inclusion_rule(
            industry, inclusions, social_inclusion, si_threshold
        ):
            return True
        return False

    def social_themes_ISS_inclusion(
        self,
        inclusions: list,
        industry: str,
        iss_key: bool,
        iss_rev: float,
        revenue_threshold: float,
        social_fin: float,
        sf_threshold: float,
        social_connect: float,
        sc_threshold: float,
        **kwargs
    ):
        """
        Check if
        (
            ISS revenue is bigger than revenue threshold
            AND
            either industry is in inclusions or company has keyword for theme in description
        )
        OR
        company derives revenue from social financing
        OR
        company derives revenue from social connectivity

        Parameters
        ----------
        inclusions: list
            list of industries for inclusion
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        iss_rev: float
            revenue calculated on ISS measures
        revenue_threshold: float
            minimum revenue threshold
        social_fin: float
            recent-year percentage of revenue company has derived from loans to small and
            medium enterprises in finance and insurance industry
        sf_threshold: float
            min revenue threshold for social financing theme
        social_connect: float
            recent-year percentage of revenue  from products, services, or infrastructure
            that support internet connectivity in the Least Developed Countries
        sc_threshold:
            min revenue threshold for social connectivity theme

        Returns
        -------
        bool
            rule is fulfilled
        """
        if (
            self.ISS_inclusion_key_rule(
                inclusions, industry, iss_key, iss_rev, revenue_threshold
            )
            or self.bigger_eq_rule(social_fin, sf_threshold)
            or self.bigger_eq_rule(social_connect, sc_threshold)
        ):
            return True
        return False

    def health_factors(
        self,
        industry: str,
        inclusions: list,
        acc_to_health: float,
        acc_threshold: float,
        trailing_rd_sales: float,
        sales_threshold: float,
        **kwargs
    ):
        """
        Check if
        (
            company improves healthcare in developing countries
            OR
            company derives revenue from RD sales
        )
        AND
        company's industry is in inclusion list

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        inclusions: list
            list of industries for inclusion
        acc_to_health: float
            companies' efforts to improve access to healthcare in developing countries
        acc_threshold: float
            min revenue threshold for healthcare improvements
        trailing_rd_sales: float
            trailing 12 month RD sales
        sales_threshold: float
            min revenue threshold for RD sales

        Returns
        -------
        bool
            rule is fulfilled
        """
        if (
            self.bigger_eq_rule(acc_to_health, acc_threshold)
            or self.bigger_eq_rule(trailing_rd_sales, sales_threshold)
        ) and self.include_rule(industry, inclusions):
            return True
        return False

    def MSCI_health_factors(
        self,
        industry: str,
        inclusions: list,
        msci_rev: float,
        revenue_threshold: float,
        acc_to_health: float,
        acc_threshold: float,
        trailing_rd_sales: float,
        sales_threshold: float,
        **kwargs
    ):
        """
        Check if
        MSCI revenue is bigger than threshold
        AND
        (
            company improves healthcare in developing countries
            OR
            company derives revenue from RD sales
        )
        AND
        company's industry is in inclusion list

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        inclusions: list
            list of industries for inclusion
        msci_rev: float
            revenue calculated on MSCI measures
        revenue_threshold: float
            minimum revenue threshold
        acc_to_health: float
            companies' efforts to improve access to healthcare in developing countries
        acc_threshold: float
            min revenue threshold for healthcare improvements
        trailing_rd_sales: float
            trailing 12 month RD sales
        sales_threshold: float
            min revenue threshold for RD sales

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.revenue_bigger_rule(
            msci_rev, revenue_threshold
        ) and self.health_factors(
            industry,
            inclusions,
            acc_to_health,
            acc_threshold,
            trailing_rd_sales,
            sales_threshold,
        ):
            return True
        return False

    def health_inclusion(
        self,
        msci_rev: float,
        revenue_threshold: float,
        industry: str,
        health1: list,
        health2: list,
        health3: list,
        orphan_drug_rev: float,
        orphan_drug_threshold: float,
        acc_to_health: float,
        acc_threshold: float,
        trailing_rd_sales: float,
        sales_threshold: float,
        **kwargs
    ):
        """
        Check if
        (
            MSCI revenue is bigger than threshold
            AND
            (
                company improves healthcare in developing countries
                OR
                company derives revenue from RD sales
            )
            AND
            company's industry is in inclusion list
        )
        OR
        (
            MSCI revenue is bigger than revenue threshold
            AND
            industry is in inclusions
        )
        OR
        company's industry is in inclusion list
        OR
        company derives revenue for drug sales to orphans

        Parameters
        ----------
        msci_rev: float
            revenue calculated on MSCI measures
        revenue_threshold: float
            minimum revenue threshold
        industry: str
            name of industry company belongs to
        health1: list
            list of industries for inclusion in health factor rule
        health2: list
            list of industries for inclusion in MSCI inclusion rule
        health3: list
            list of industries for inclusion in general rule
        orphan_drug_rev: float
            recent-year absolute revenue from drugs for orphan/neglected diseases
        orphan_drug_threshold: float
            min revenue threshold for drugs for orphans
        acc_to_health: float
            companies' efforts to improve access to healthcare in developing countries
        acc_threshold: float
            min revenue threshold for healthcare improvements
        trailing_rd_sales: float
            trailing 12 month RD sales
        sales_threshold: float
            min revenue threshold for RD sales

        Returns
        -------
        bool
            rule is fulfilled
        """
        if (
            self.MSCI_health_factors(
                industry,
                health1,
                msci_rev,
                revenue_threshold,
                acc_to_health,
                acc_threshold,
                trailing_rd_sales,
                sales_threshold,
            )
            or self.MSCI_inclusion_rule(health2, industry, msci_rev, revenue_threshold)
            or self.include_rule(industry, health3)
            or self.bigger_eq_rule(orphan_drug_rev, orphan_drug_threshold)
        ):
            return True
        return False

    def RENEWENERGY(
        self,
        industry: str,
        iss_key: bool,
        msci_rev: float,
        iss_rev: float,
        capex: float,
        **kwargs
    ):
        """
        Check if a company should be assigned to RENEWENERGY theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        capex: float
            capex

        Returns
        -------
        bool
            rule is fulfilled

        """
        if not iss_key:
            iss_rev = 0

        if (
            self.exclude_rule(industry, self.params["ALL_exclusions"])
            and self.exclusion_key_rule(
                industry=industry, iss_key=iss_key, **self.params["RENEWENERGY"]
            )
            and self.revenue_rule(
                msci_rev=msci_rev,
                iss_rev=iss_rev,
                capex=capex,
                **self.params["RENEWENERGY"]
            )
        ):
            return True
        return False

    def MOBILITY(
        self,
        industry: str,
        iss_key: bool,
        msci_rev: float,
        iss_rev: float,
        capex: float,
        **kwargs
    ):
        """
        Check if a company should be assigned to MOBILITY theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        capex: float
            capex

        Returns
        -------
        bool
            rule is fulfilled
        """
        if not iss_key:
            iss_rev = 0

        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.MSCI_ISS_inclusion_key_capex_rule(
            industry=industry,
            iss_key=iss_key,
            msci_rev=msci_rev,
            iss_rev=iss_rev,
            capex=capex,
            **self.params["MOBILITY"]
        ):
            return True
        return False

    def CIRCULARITY(
        self, industry: str, iss_key: bool, msci_rev: float, iss_rev: float, **kwargs
    ):
        """
        Check if a company should be assigned to CIRCULARITY theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.MSCI_ISS_inclusion_key_rule(
            industry=industry,
            iss_key=iss_key,
            msci_rev=msci_rev,
            iss_rev=iss_rev,
            **self.params["CIRCULARITY"]
        ):
            return True
        return False

    def CCADAPT(self, industry: str, msci_rev: float, iss_rev: float, **kwargs):
        """
        Check if a company should be assigned to CCADAPT theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.min_revenue_rule(
            msci_rev=msci_rev, iss_rev=iss_rev, **self.params["CCADAPT"]
        ):
            return True
        return False

    def BIODIVERSITY(
        self, industry: str, msci_rev: float, iss_rev: float, palm_tie: bool, **kwargs
    ):
        """
        Check if a company should be assigned to BIODIVERSITY theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures
        palm_tie: bool
            producer, distributor of palm oil

        Returns
        -------
        bool
            rule is fulfilled
        """
        if (
            self.exclude_rule(industry, self.params["ALL_exclusions"])
            and self.exclude_rule(industry=industry, **self.params["BIODIVERSITY"])
            and self.reverse_bool_rule(b=palm_tie)
            and self.min_revenue_rule(
                msci_rev=msci_rev, iss_rev=iss_rev, **self.params["BIODIVERSITY"]
            )
        ):
            return True
        return False

    def SMARTCITIES(
        self, industry: str, iss_key: bool, msci_rev: float, iss_rev: float, **kwargs
    ):
        """
        Check if a company should be assigned to RENEWENERGY theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures

        Returns
        -------
        bool
            rule is fulfilled
        """
        if not iss_key:
            iss_rev = 0

        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.MSCI_ISS_inclusion_key_rule(
            industry=industry,
            iss_key=iss_key,
            msci_rev=msci_rev,
            iss_rev=iss_rev,
            **self.params["SMARTCITIES"]
        ):
            return True
        return False

    def EDU(self, industry: str, msci_rev: float, iss_rev: float, **kwargs):
        """
        Check if a company should be assigned to EDU theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.min_revenue_rule(
            msci_rev=msci_rev, iss_rev=iss_rev, **self.params["EDU"]
        ):
            return True
        return False

    def HEALTH(
        self,
        industry: str,
        msci_rev: float,
        orphan_drug_rev: float,
        acc_to_health: float,
        trailing_rd_sales: float,
        **kwargs
    ):
        """
        Check if a company should be assigned to HEALTH theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        orphan_drug_rev: float
            recent-year absolute revenue from drugs for orphan/neglected diseases
        acc_to_health: float
            companies' efforts to improve access to healthcare in developing countries
        trailing_rd_sales: float
            trailing 12 month RD sales

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.health_inclusion(
            msci_rev=msci_rev,
            industry=industry,
            orphan_drug_rev=orphan_drug_rev,
            acc_to_health=acc_to_health,
            trailing_rd_sales=trailing_rd_sales,
            **self.params["HEALTH"]
        ):
            return True
        return False

    def SANITATION(self, industry: str, msci_rev: float, iss_rev: float, **kwargs):
        """
        Check if a company should be assigned to SANITATION theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.min_revenue_rule(
            msci_rev=msci_rev, iss_rev=iss_rev, **self.params["SANITATION"]
        ):
            return True
        return False

    def INCLUSION(
        self,
        industry: str,
        iss_key: bool,
        iss_rev: float,
        social_fin: float,
        social_connect: float,
        **kwargs
    ):
        """
        Check if a company should be assigned to INCLUSION theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        iss_key: bool
            boolean: company has theme specific word in description
        iss_rev: float
            revenue calculated on ISS measures
        social_fin: float
            recent-year percentage of revenue company has derived from loans to small and
            medium enterprises in finance and insurance industry
        social_connect: float
            recent-year percentage of revenue  from products, services, or infrastructure
            that support internet connectivity in the Least Developed Countries

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.social_themes_ISS_inclusion(
            industry=industry,
            iss_key=iss_key,
            iss_rev=iss_rev,
            social_fin=social_fin,
            social_connect=social_connect,
            **self.params["INCLUSION"]
        ):
            return True
        return False

    def NUTRITION(self, industry: str, msci_rev: float, iss_rev: float, **kwargs):
        """
        Check if a company should be assigned to NUTRITION theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        iss_rev: float
            revenue calculated on ISS measures

        Returns
        -------
        bool
            rule is fulfilled
        """
        if (
            self.exclude_rule(industry, self.params["ALL_exclusions"])
            and self.exclude_rule(industry, **self.params["NUTRITION"])
            and self.min_revenue_rule(
                msci_rev=msci_rev, iss_rev=iss_rev, **self.params["NUTRITION"]
            )
        ):
            return True
        return False

    def AFFORDABLE(
        self, industry: str, msci_rev: float, social_inclusion: float, **kwargs
    ):
        """
        Check if a company should be assigned to AFFORDABLE theme

        Parameters
        ----------
        industry: str
            name of industry company belongs to
        msci_rev: float
            revenue calculated on MSCI measures
        social_inclusion: float
            total of all revenues derived from any of the basic needs social impact themes

        Returns
        -------
        bool
            rule is fulfilled
        """
        if self.exclude_rule(
            industry, self.params["ALL_exclusions"]
        ) and self.MSCI_social_impact_revenue_inclusion_rule(
            industry=industry,
            msci_rev=msci_rev,
            social_inclusion=social_inclusion,
            **self.params["AFFORDABLE"]
        ):
            return True
        return False
