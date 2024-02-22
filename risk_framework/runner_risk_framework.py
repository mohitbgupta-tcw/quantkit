import pandas as pd
import quantkit.runner as loader
import quantkit.utils.logging as logging
import quantkit.risk_framework.data_loader.themes_datasource as themes_datasource
import quantkit.risk_framework.data_loader.category_datasource as category_database
import quantkit.risk_framework.data_loader.sector_datasource as sector_database
import quantkit.risk_framework.data_loader.transition_datasource as transition_datasource
import quantkit.risk_framework.data_loader.parent_issuer_datasource as pi_datasource
import quantkit.risk_framework.data_loader.sdg_datasource as sdg_datasource
import quantkit.risk_framework.data_loader.securitized_datasource as securitized_datasource
import quantkit.risk_framework.data_loader.exclusions_datasource as exclusions_database
import quantkit.risk_framework.data_loader.adjustment_datasource as adjustment_database
import quantkit.risk_framework.data_loader.r_and_d_datasource as r_and_d_datasource
import quantkit.risk_framework.data_loader.portfolio_datasource as portfolio_datasource


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

        # connect themes datasource
        theme_calculations = self.params.get("theme_calculation", dict())
        self.theme_datasource = themes_datasource.ThemeDataSource(
            params=self.params["theme_datasource"],
            theme_calculations=theme_calculations,
            api_settings=self.api_settings,
        )

        # connect category datasource
        self.category_datasource = category_database.CategoryDataSource(
            params=self.params["category_datasource"], api_settings=self.api_settings
        )

        # connect sector datasource
        self.sector_datasource = sector_database.SectorDataSource(
            params=self.params["sector_datasource"], api_settings=self.api_settings
        )

        transition_params = self.params.get("transition_parameters", dict())
        # connect BCLASS datasource
        self.bclass_datasource = sector_database.BClassDataSource(
            params=self.params["bclass_datasource"],
            transition_params=transition_params,
            api_settings=self.api_settings,
        )

        # connect GICS datasource
        self.gics_datasource = sector_database.GICSDataSource(
            params=self.params["gics_datasource"],
            transition_params=transition_params,
            api_settings=self.api_settings,
        )

        # connect transition datasource
        self.transition_datasource = transition_datasource.TransitionDataSource(
            params=self.params["transition_datasource"], api_settings=self.api_settings
        )

        # connect parent issuer datasource
        self.parent_issuer_datasource = pi_datasource.ParentIssuerSource(
            params=self.params["parent_issuer_datasource"],
            api_settings=self.api_settings,
        )

        # connect SDG datasource
        self.sdg_datasource = sdg_datasource.SDGDataSource(
            params=self.params["sdg_datasource"], api_settings=self.api_settings
        )

        # connect securitized mapping datasource
        self.securitized_datasource = securitized_datasource.SecuritizedDataSource(
            params=self.params["securitized_datasource"], api_settings=self.api_settings
        )

        # connect exclusion datasource
        self.exclusion_datasource = exclusions_database.ExclusionsDataSource(
            params=self.params["exclusion_datasource"], api_settings=self.api_settings
        )

        # connect analyst adjustment datasource
        self.adjustment_datasource = adjustment_database.AdjustmentDataSource(
            params=self.params["adjustment_datasource"], api_settings=self.api_settings
        )

        # connect bloomberg datasource
        self.r_and_d_datasource = r_and_d_datasource.RandDDataSource(
            params=self.params["bloomberg_datasource"], api_settings=self.api_settings
        )

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
        self.iter_r_and_d()
        self.iter_sdg()
        self.iter_msci()
        self.iter_adjustment()
        self.iter_exclusion()
        self.iter_holdings()
        self.iter_securities()
        self.iter_cash()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()

    def iter_themes(self) -> None:
        """
        - load theme data
        - create Theme objects for each theme
        """
        self.theme_datasource.load()
        self.theme_datasource.iter()

    def iter_sectors(self) -> None:
        """
        - create Sector objects
        - create BClass4 objects and attached Industry object
        - map transition targets to sub sectors
        """
        self.sector_datasource.load()
        self.sector_datasource.iter()

        # create Sub-Sector objects for BCLASS
        self.bclass_datasource.load()
        self.bclass_datasource.iter()

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
        self.gics_datasource.iter()

        # assign Sub Sector object to Sector and vice verse
        for g in self.gics_datasource.gics:
            self.sector_datasource.sectors["GICS"].add_sub_sector(
                self.gics_datasource.gics[g]
            )
            self.gics_datasource.gics[g].add_sector(
                self.sector_datasource.sectors["GICS"]
            )

        # map transition target and transition revenue to each sub-sector
        self.iter_transitions()

    def iter_category(self):
        """
        load category data
        """
        self.category_datasource.load()
        self.category_datasource.iter()

    def iter_transitions(self) -> None:
        """
        - load transition data
        """
        self.transition_datasource.load()
        self.transition_datasource.iter(
            self.gics_datasource.gics, self.bclass_datasource.bclass
        )

    def iter_securitized_mapping(self) -> None:
        """
        Iterate over the securitized mapping
        """
        self.securitized_datasource.load()
        self.securitized_datasource.iter()

    def iter_adjustment(self) -> None:
        """
        iterate over adjustment data
        """
        # load adjustment data
        self.adjustment_datasource.load()
        self.adjustment_datasource.iter()

    def iter_exclusion(self) -> None:
        """
        iterate over exclusion data
        """
        issuer_ids = self.portfolio_datasource.all_msci_ids
        self.params["exclusion_datasource"]["filters"][
            "issuer_identifier_list"
        ] = issuer_ids

        # load exclusion data
        self.exclusion_datasource.load()
        self.exclusion_datasource.iter()

    def iter_parent_issuers(self) -> None:
        """
        iterate over parent issuers
        """
        self.parent_issuer_datasource.load()
        self.parent_issuer_datasource.iter()

    def iter_msci(self) -> None:
        """
        iterate over MSCI data
        """
        # load parent issuer data
        parent_ids = self.parent_issuer_datasource.parent_issuer_ids()

        # load MSCI data
        issuer_ids = self.portfolio_datasource.all_msci_ids

        issuer_ids += parent_ids
        issuer_ids = list(set(issuer_ids))
        self.params["msci_datasource"]["filters"]["issuer_identifier_list"] = issuer_ids
        self.msci_datasource.load()
        self.msci_datasource.iter()

    def iter_sdg(self) -> None:
        """
        iterate over SDG data
        """
        self.sdg_datasource.load(
            as_of_date=self.params["portfolio_datasource"]["end_date"],
        )
        self.sdg_datasource.iter()

    def iter_r_and_d(self) -> None:
        """
        iterate over research and development data
        """
        self.r_and_d_datasource.load()
        self.r_and_d_datasource.iter()

    def iter_securities(self) -> None:
        """
        - Overwrite Parent
        - Add ESG Collateral Type
        - Attach BClass Level4 to parent
        - Attach Sector Level 2
        - Attach Analyst Adjustment
        - Attach Bloomberg information
        - Attach ISS information
        """
        logging.log("Iterate Securities")
        for sec, sec_store in self.portfolio_datasource.securities.items():
            sec_store.iter(
                parent_issuer_dict=self.parent_issuer_datasource.parent_issuers,
                companies=self.portfolio_datasource.companies,
                securitized_mapping=self.securitized_datasource.securitized_mapping,
                bclass_dict=self.bclass_datasource.bclass,
                sec_adjustment_dict=self.adjustment_datasource.security_isins,
                rud_dict=self.r_and_d_datasource.research_development,
                sdg_dict=self.sdg_datasource.sdg,
            )

    def iter_securitized(self) -> None:
        """
        Iterate over all Securitized
        """
        logging.log("Iterate Securitized")
        for sec, sec_store in self.portfolio_datasource.securitized.items():
            sec_store.iter(
                regions=self.region_datasource.regions,
                gics_d=self.gics_datasource.gics,
                bclass_d=self.bclass_datasource.bclass,
                exclusion_dict=self.exclusion_datasource.exclusions,
            )

    def iter_sovereigns(self) -> None:
        """
        Iterate over all sovereigns
        """
        logging.log("Iterate Sovereigns")
        for s, sov_store in self.portfolio_datasource.sovereigns.items():
            sov_store.iter(
                regions=self.region_datasource.regions,
                msci_adjustment_dict=self.adjustment_datasource.msci_ids,
                gics_d=self.gics_datasource.gics,
                bclass_d=self.bclass_datasource.bclass,
                exclusion_dict=self.exclusion_datasource.exclusions,
            )

    def iter_muni(self) -> None:
        """
        Iterate over all Munis
        """
        logging.log("Iterate Munis")
        for m, muni_store in self.portfolio_datasource.munis.items():
            muni_store.iter(
                regions=self.region_datasource.regions,
                gics_d=self.gics_datasource.gics,
                bclass_d=self.bclass_datasource.bclass,
                exclusion_dict=self.exclusion_datasource.exclusions,
            )

    def iter_cash(self) -> None:
        """
        Iterate over all Cash objects
        """
        logging.log("Iterate Cash")
        for c, cash_store in self.portfolio_datasource.cash.items():
            cash_store.iter(
                regions=self.region_datasource.regions,
                gics_d=self.gics_datasource.gics,
                bclass_d=self.bclass_datasource.bclass,
                exclusion_dict=self.exclusion_datasource.exclusions,
            )

    def iter_companies(self) -> None:
        """
        Iterate over all companies
        """
        logging.log("Iterate Companies")
        for c, comp_store in self.portfolio_datasource.companies.items():
            comp_store.iter(
                companies=self.portfolio_datasource.companies,
                regions=self.region_datasource.regions,
                exclusion_dict=self.exclusion_datasource.exclusions,
                gics_d=self.gics_datasource.gics,
                bclass_d=self.bclass_datasource.bclass,
                category_d=self.category_datasource.categories,
                msci_adjustment_dict=self.adjustment_datasource.msci_ids,
            )
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
