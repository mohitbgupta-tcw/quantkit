import pandas as pd
import quantkit.loader.runner as loader
import quantkit.utils.logging as logging


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
        super().init(local_configs)

        # iterate over dataframes and create objects
        logging.log("Start Iterating")
        self.iter()

    def iter(self) -> None:
        """
        iterate over DataFrames and create connected objects
        """
        self.iter_themes()
        self.iter_regions()
        self.iter_sectors()
        self.iter_category()
        self.iter_securitized_mapping()
        self.iter_parent_issuers()
        self.iter_portfolios()
        self.iter_bloomberg()
        self.iter_sdg()
        self.iter_msci()
        self.iter_quandl()
        self.iter_adjustment()
        self.iter_exclusion()
        self.iter_holdings()
        self.iter_securities()
        self.iter_cash()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()

    def iter_companies(self) -> None:
        """
        Iterate over all companies
        """
        super().iter_companies()
        self.company_calculations()
        self.replace_transition_risk()

    def company_calculations(self):
        """
        Multiple Company calculations
        """
        for c, comp_store in self.portfolio_datasource.companies.items():
            # calculate capex
            comp_store.calculate_capex()

            # calculate climate revenue
            comp_store.calculate_climate_revenue()

            # calculate carbon intensite --> if na, assign industry median
            comp_store.calculate_carbon_intensity()

            # assign theme and Sustainability_Tag
            comp_store.check_theme_requirements(self.theme_datasource.themes)

    def replace_transition_risk(self) -> None:
        """
        Split companies with unassigned industry and sub-industry into
        high and low transition risk
        --> check if carbon intensity is bigger or smaller than predefined threshold
        """
        # create new Industry objects for Unassigned High and Low

        for c, store in self.bclass_datasource.industries[
            "Unassigned BCLASS"
        ].companies.items():
            if store.type == "company":
                store.replace_unassigned_industry(
                    high_threshold=self.params["transition_parameters"][
                        "High_Threshold"
                    ],
                    industries=self.bclass_datasource.industries,
                )

    def iter_portfolios(self) -> None:
        """
        - load portfolio data
        - create portfolio objects
        - attach Sector to Portfolio object
        """
        super().iter_portfolios()
        self.sector_datasource.iter_portfolios(self.portfolio_datasource.portfolios)

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
            comp_store.update_sovereign_score()
            comp_store.calculate_transition_score()
            comp_store.calculate_esrm_score()
            comp_store.iter_analyst_adjustment(self.theme_datasource.themes)
            comp_store.calculate_corporate_score()
            comp_store.calculate_risk_overall_score()
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
            sov_store.update_sovereign_score()
            sov_store.iter_analyst_adjustment(self.theme_datasource.themes)
            sov_store.calculate_risk_overall_score()
            sov_store.update_sclass()

    def calculate_securitized_scores(self) -> None:
        """
        Calculate scores for each Securitzed:
            - securitized score
            - risk overall score
            - SClass
        """
        for sec, sec_store in self.portfolio_datasource.securitized.items():
            sec_store.calculate_securitized_score(
                green=self.securitized_datasource.green,
                social=self.securitized_datasource.social,
                sustainable=self.securitized_datasource.sustainable,
                clo=self.securitized_datasource.clo,
            )
            sec_store.iter_analyst_adjustment(self.theme_datasource.themes)
            sec_store.calculate_risk_overall_score()
            sec_store.update_sclass()

    def calculate_muni_scores(self) -> None:
        """
        Calculate scores for each Muni:
            - analyst adjustment
            - risk overall score
            - SClass
        """
        for muni, muni_store in self.portfolio_datasource.munis.items():
            muni_store.iter_analyst_adjustment(self.theme_datasource.themes)
            muni_store.calculate_risk_overall_score()
            muni_store.update_sclass()

    def calculate_cash_scores(self) -> None:
        """
        Calculate scores for Cash:
            - analyst adjustment
            - risk overall score
            - SClass
        """
        for cash, cash_store in self.portfolio_datasource.cash.items():
            cash_store.iter_analyst_adjustment(self.theme_datasource.themes)
            cash_store.calculate_risk_overall_score()
            cash_store.update_sclass()

    def run(self) -> None:
        """
        run calculations
        """
        logging.log("Start Calculations")
        self.calculate_company_scores()
        self.calculate_securitized_scores()
        self.calculate_sovereign_scores()
        self.calculate_muni_scores()
        self.calculate_cash_scores()
