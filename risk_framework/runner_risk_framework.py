import pandas as pd
import quantkit.runner as loader
import quantkit.utils.logging as logging
import quantkit.risk_framework.data_loader.portfolio_datasource as portfolio_datasource
import quantkit.risk_framework.data_loader.security_datasource as security_datasource


class Runner(loader.Runner):
    def init(self, local_configs: str = ""):
        """
        - initialize datsources and load data
        - create reusable attributes
        - itereate over DataFrames and create connected objects

        Parameters
        ----------
        local_configs: str, optional
            path to a local configarations file
        """
        super().init(local_configs, "risk_framework")

        # connect portfolio datasource
        self.portfolio_datasource = portfolio_datasource.PortfolioDataSource(
            params=self.params["portfolio_datasource"], api_settings=self.api_settings
        )

        # connect security datasource
        self.security_datasource = security_datasource.SecurityDataSource(
            params=self.params["security_datasource"], api_settings=self.api_settings
        )

        # iterate over dataframes and create objects
        logging.log("Start Iterating")
        self.iter()

    def iter(self) -> None:
        """
        iterate over DataFrames and create connected objects
        """
        self.iter_parent_issuers()
        self.iter_portfolios()
        self.iter_securities()
        self.iter_msci_esg()
        self.iter_sdg()
        self.iter_r_and_d()
        self.iter_themes()
        self.iter_regions()
        self.iter_sectors()
        self.iter_transitions()
        self.iter_category()
        self.iter_securitized_mapping()
        self.iter_adjustment()
        self.iter_exclusion()

    def iter_themes(self) -> None:
        """
        - load theme data
        - create Theme objects for each theme
        """
        self.theme_datasource.load()
        self.theme_datasource.iter()

    def iter_regions(self) -> None:
        """
        - load region data
        - create region objects and save in dict
        """
        self.region_datasource.load()
        self.region_datasource.iter(self.security_datasource.securities)

    def iter_sectors(self) -> None:
        """
        - create Sector objects
        - create BClass4 objects and attached Industry object
        - map transition targets to sub sectors
        """
        self.sector_datasource.load()
        self.sector_datasource.iter(self.portfolio_datasource.portfolios)

        # create Sub-Sector objects for BCLASS
        self.bclass_datasource.load()
        self.bclass_datasource.iter(self.security_datasource.issuers)

        # assign Sub Sector object to Sector and vice verse
        for bc in self.bclass_datasource.bclass:
            self.sector_datasource.sectors["BCLASS"].add_sub_sector(
                self.bclass_datasource.bclass[bc]
            )
            self.bclass_datasource.bclass[bc].add_sector(
                self.sector_datasource.sectors["BCLASS"]
            )

        # create Sub-Sector objects for GICS
        self.gics_datasource.load()
        self.gics_datasource.iter(self.security_datasource.issuers)

        # assign Sub Sector object to Sector and vice verse
        for g in self.gics_datasource.gics:
            self.sector_datasource.sectors["GICS"].add_sub_sector(
                self.gics_datasource.gics[g]
            )
            self.gics_datasource.gics[g].add_sector(
                self.sector_datasource.sectors["GICS"]
            )

        for iss, issuer_store in self.security_datasource.issuers.items():
            issuer_store.attach_industry(
                self.gics_datasource.gics, self.bclass_datasource.bclass
            )

    def iter_category(self):
        """
        load category data
        """
        self.category_datasource.load()
        self.category_datasource.iter()

    def iter_transitions(self) -> None:
        """
        - load transition data
        - load company mapping for transition
        """
        self.transition_company_datasource.load()
        self.transition_company_datasource.iter(self.security_datasource.issuers)

    def iter_securitized_mapping(self) -> None:
        """
        Iterate over the securitized mapping
        """
        self.securitized_datasource.load()
        self.securitized_datasource.iter(self.security_datasource.securities)

    def iter_adjustment(self) -> None:
        """
        iterate over adjustment data
        """
        # load adjustment data
        self.adjustment_datasource.load()
        self.adjustment_datasource.iter(self.security_datasource.issuers)

    def iter_exclusion(self) -> None:
        """
        iterate over exclusion data
        """
        issuer_ids = self.security_datasource.msci_ids
        self.params["exclusion_datasource"]["filters"][
            "issuer_identifier_list"
        ] = issuer_ids

        # load exclusion data
        self.exclusion_datasource.load()
        self.exclusion_datasource.iter(self.security_datasource.issuers)

    def iter_parent_issuers(self) -> None:
        """
        iterate over parent issuers
        """
        self.parent_issuer_datasource.load()
        self.parent_issuer_datasource.iter()
        self.params["security_datasource"]["transformation"].update(
            self.parent_issuer_datasource.parent_issuers
        )

    def iter_msci_esg(self) -> None:
        """
        iterate over MSCI data
        """
        # load MSCI data
        issuer_ids = self.security_datasource.msci_ids

        self.params["msci_datasource"]["filters"]["issuer_identifier_list"] = issuer_ids
        self.msci_esg_datasource.load()
        self.msci_esg_datasource.iter(self.security_datasource.issuers)

    def iter_sdg(self) -> None:
        """
        iterate over SDG data
        """
        self.sdg_datasource.load(
            as_of_date=self.params["portfolio_datasource"]["end_date"],
        )
        self.sdg_datasource.iter(self.security_datasource.issuers)

    def iter_r_and_d(self) -> None:
        """
        iterate over research and development data
        """
        self.r_and_d_datasource.load()
        self.r_and_d_datasource.iter(self.security_datasource.issuers)

    def calculate_company_scores(self) -> None:
        """
        Calculuate scores for each company:
            - sovereign score (for treasury)
            - esrm score
            - governance score
            - transition score
            - analyst adjustment
            - corporate score
            - risk overall score
            - SClass
        """
        for c, comp_store in self.portfolio_datasource.companies.items():
            comp_store.update_sclass()

    def calculate_sovereign_scores(self) -> None:
        """
        Calculate scores for each sovereign:
            - sovereign score
            - analyst adjustment
            - risk overall score
            - SClass
        """
        for sov, sov_store in self.portfolio_datasource.sovereigns.items():
            sov_store.update_sclass()

    def calculate_securitized_scores(self) -> None:
        """
        Calculate scores for each Securitzed:
            - securitized score
            - risk overall score
            - SClass
        """
        for sec, sec_store in self.portfolio_datasource.securitized.items():
            sec_store.update_sclass()

    def calculate_muni_scores(self) -> None:
        """
        Calculate scores for each Muni:
            - analyst adjustment
            - risk overall score
            - SClass
        """
        for muni, muni_store in self.portfolio_datasource.munis.items():
            muni_store.update_sclass()

    def calculate_cash_scores(self) -> None:
        """
        Calculate scores for Cash:
            - analyst adjustment
            - risk overall score
            - SClass
        """
        for cash, cash_store in self.portfolio_datasource.cash.items():
            cash_store.update_sclass()

    def risk_framework_calculation(self) -> None:
        """
        Risk Framework specific calculations:
            - issuer CapEx
            - issuer climate revenue
            - issuer decarbonization factor
            - issuer carbon intensity
            - issuer transition information
        """
        for iss, issuer_store in self.security_datasource.issuers.items():
            issuer_store.calculate_capex()
            issuer_store.calculate_climate_revenue()
            issuer_store.calculate_climate_decarb()
            issuer_store.calculate_carbon_intensity()

    def risk_framework_scores(self) -> None:
        """
        Calculate Risk Framework Scores and Themes
        """
        for sec, sec_store in self.security_datasource.securities.items():
            sec_store.calculate_sustainable_theme(self.theme_datasource.themes)
            # sec_store.calculate_transition_score()
            # sec_store.calculate_sovereign_score()
            # sec_store.calculate_esrm_score()
            sec_store.iter_analyst_adjustment(self.theme_datasource.themes)
            sec_store.calculate_security_score()
            sec_store.calculate_risk_overall_score()

    def run(self) -> None:
        """
        run calculations
        """
        logging.log("Start Calculations")
        self.risk_framework_calculation()
        self.risk_framework_scores()

        raise NotImplementedError()

        self.calculate_company_scores()
        self.calculate_securitized_scores()
        self.calculate_sovereign_scores()
        self.calculate_muni_scores()
        self.calculate_cash_scores()
