import pandas as pd
import quantkit.loader.runner as loader
import quantkit.utils.logging as logging


class Runner(loader.Runner):
    def init(self):
        """
        - initialize datsources and load data
        - create reusable attributes
        - itereate over DataFrames and create connected objects
        """
        super().init()

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
        self.iter_securitized_mapping()
        self.iter_portfolios()
        self.iter_securities()
        self.iter_holdings()
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

    def iter_regions(self) -> None:
        """
        - load region data
        - create region objects and save in dict
        """
        self.region_datasource.load()
        self.region_datasource.iter()

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

    def iter_transitions(self) -> None:
        """
        - load transition data
        """
        self.transition_datasource.load()
        self.transition_datasource.iter(
            self.gics_datasource.gics, self.bclass_datasource.bclass
        )

    def iter_portfolios(self) -> None:
        """
        - load portfolio data
        - create portfolio objects
        - attach Sector to Portfolio object
        """
        self.portfolio_datasource.load()
        self.portfolio_datasource.iter()

        # attach sector to portfolio
        self.sector_datasource.iter_portfolios(self.portfolio_datasource.portfolios)

    def iter_securities(self) -> None:
        """
        - create Company object for each security with key ISIN
        - create Security object with key Security ISIN
        - attach analyst adjustment based on sec isin
        """
        # load security data
        self.security_datasource.load()

        # load parent issuer data
        self.parent_issuer_datasource.load()
        parent_ids = self.parent_issuer_datasource.parent_issuer_ids()

        # load MSCI data
        issuer_ids = self.security_datasource.issuer_ids(
            self.portfolio_datasource.all_holdings
        )
        issuer_ids += parent_ids
        issuer_ids = list(set(issuer_ids))
        self.params["msci_datasource"]["filters"]["issuer_identifier_list"] = issuer_ids
        self.msci_datasource.load()

        # load adjustment data
        self.adjustment_datasource.load()

        # load exclusion data
        self.exclusion_datasource.load()

        logging.log("Iterate Securities")
        # only iterate over securities the portfolios actually hold to save time
        self.security_datasource.iter(
            securities=self.portfolio_datasource.all_holdings,
            companies=self.portfolio_datasource.companies,
            df_portfolio=self.portfolio_datasource.df,
            msci_df=self.msci_datasource.df,
            adjustment_df=self.adjustment_datasource.df,
        )

    def iter_securitized_mapping(self) -> None:
        """
        Iterate over the securitized mapping
        """
        self.securitized_datasource.load()
        self.securitized_datasource.iter()

    def iter_holdings(self) -> None:
        """
        Iterate over portfolio holdings
        - attach ESG information so security
        - create Muni, Sovereign, Securitized objects
        - attach sector information to company
        - attach BCLASS to company
        - attach MSCI rating to company
        - attach holdings, OAS to self.holdings with security object
        """
        self.portfolio_datasource.iter_holdings(
            self.security_datasource.securities,
            self.securitized_datasource.securitized_mapping,
            self.bclass_datasource.bclass,
        )

    def iter_sdg(self) -> None:
        """
        iterate over SDG data
        - attach sdg information to company in self.sdg_information
        - if company doesn't have data, attach all nan's
        """
        # load SDG data
        self.sdg_datasource.load()
        self.sdg_datasource.iter(
            self.portfolio_datasource.companies,
            self.portfolio_datasource.munis,
            self.portfolio_datasource.sovereigns,
            self.portfolio_datasource.securitized,
        )

    def iter_bloomberg(self) -> None:
        """
        iterate over bloomberg data
        - attach bloomberg information to company in self.bloomberg_information
        - if company doesn't have data, attach all nan's
        """
        # load bloomberg data
        self.bloomberg_datasource.load()
        self.bloomberg_datasource.iter(self.portfolio_datasource.companies)

    def iter_quandl(self) -> None:
        """
        iterate over quandl data
        - attach quandl information to company in self.quandl_information
        - if company doesn't have data, attach all nan's
        """
        # load quandl data
        self.quandl_datasource.load(self.security_datasource.all_tickers)
        self.quandl_datasource.iter(self.portfolio_datasource.companies)

    def iter_sovereigns(self) -> None:
        """
        Iterate over all sovereigns
        """
        logging.log("Iterate Sovereigns")
        for s in self.portfolio_datasource.sovereigns:
            self.portfolio_datasource.sovereigns[s].iter(
                regions_df=self.region_datasource.df,
                regions=self.region_datasource.regions,
                adjustment_df=self.adjustment_datasource.df,
                gics_d=self.gics_datasource.gics,
            )

    def iter_securitized(self) -> None:
        """
        Iterate over all Securitized
        """
        logging.log("Iterate Securitized")
        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].iter(
                regions_df=self.region_datasource.df,
                regions=self.region_datasource.regions,
                gics_d=self.gics_datasource.gics,
            )

    def iter_muni(self) -> None:
        """
        Iterate over all Munis
        """
        logging.log("Iterate Munis")
        for m in self.portfolio_datasource.munis:
            self.portfolio_datasource.munis[m].iter(
                regions_df=self.region_datasource.df,
                regions=self.region_datasource.regions,
                gics_d=self.gics_datasource.gics,
            )

    def iter_companies(self) -> None:
        """
        Iterate over all companies
        """
        logging.log("Iterate Companies")

        # attach sdg information
        self.iter_sdg()

        # attach bloomberg information
        self.iter_bloomberg()

        # attach quandl information
        self.iter_quandl()

        # attach parent issuer id --> manually added parents from file
        self.attach_parent_issuer()

        # load category data
        self.category_datasource.load()
        self.category_datasource.iter()

        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].iter(
                companies=self.portfolio_datasource.companies,
                regions_df=self.region_datasource.df,
                regions=self.region_datasource.regions,
                exclusion_df=self.exclusion_datasource.df,
                gics_d=self.gics_datasource.gics,
                bclass_d=self.bclass_datasource.bclass,
                category_d=self.category_datasource.categories,
                adjustment_df=self.adjustment_datasource.df,
                themes=self.theme_datasource.themes,
            )

        self.replace_carbon_median()
        self.replace_transition_risk()

    def attach_parent_issuer(self) -> None:
        """
        Manually add parent issuer for selected securities
        """
        self.parent_issuer_datasource.iter(
            self.portfolio_datasource.companies, self.security_datasource.securities
        )

    def replace_carbon_median(self) -> None:
        """
        For companies without 'Carbon Intensity (Scope 123)'
        --> (CARBON_EMISSIONS_SCOPE123 / SALES_USD_RECENT) couldnt be calculuated
        --> replace NA with company's industry median
        """
        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].replace_carbon_median()

    def replace_transition_risk(self) -> None:
        """
        Split companies with unassigned industry and sub-industry into
        high and low transition risk
        --> check if carbon intensity is bigger or smaller than predefined threshold
        """
        # create new Industry objects for Unassigned High and Low

        for c in self.bclass_datasource.industries["Unassigned BCLASS"].companies:
            self.portfolio_datasource.companies[c].replace_unassigned_industry(
                high_threshold=self.params["transition_parameters"]["High_Threshold"],
                industries=self.bclass_datasource.industries,
            )

    def calculate_securitized_score(self) -> None:
        """
        Calculation of Securitized Score
        """
        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].calculate_securitized_score()

    def analyst_adjustment(self) -> None:
        """
        Do analyst adjustments for each company.
        Different calculations for each thematic type:
            - Risk
            - Transition
            - People
            - Planet
        See quantkit.finance.adjustments for more information
        """
        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].iter_analyst_adjustment(
                self.theme_datasource.themes
            )

        for sov in self.portfolio_datasource.sovereigns:
            self.portfolio_datasource.sovereigns[sov].iter_analyst_adjustment(
                self.theme_datasource.themes
            )

        for muni in self.portfolio_datasource.munis:
            self.portfolio_datasource.munis[muni].iter_analyst_adjustment(
                self.theme_datasource.themes
            )

        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].iter_analyst_adjustment(
                self.theme_datasource.themes
            )

    def calculate_esrm_score(self) -> None:
        """
        Calculuate esrm score for each company:
        1) For each category save indicator fields and EM and DM flag scorings
        2) For each company:
            2.1) Get ESRM Module (category)
            2.2) Iterate over category indicator fields and
                - count number of flags based on operator and flag threshold
                - save flag value in indicator_Flag
                - create ESRM score based on flag scorings and region
            2.3) Create Governance_Score based on Region_Theme
            2.4) Save flags in company_information
        """

        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].calculate_esrm_score()

    def calculate_transition_score(self) -> None:
        """
        Calculate transition score (Transition_Score) for each company:
        0) Check if company is excempted --> set score to 0
        1) Create transition tags
        2) Calculate target score
        3) Calculate transition score
            3.1) Start with industry initial score (3 for low, 5 for high risk)
            3.2) Subtract target score
            3.3) Subtract carbon intensity credit:
                - if in lowest quantile of industry: -2
                - if in medium quantile of industry: -1
        """
        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].calculate_transition_score()

    def calculate_corporate_score(self) -> None:
        """
        Calculate corporate score for a company based on other scores.
        Calculation:

            (Governance Score + ESRM Score + Transition Score) / 3
        """
        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].calculate_corporate_score()

    def calculate_risk_overall_score(self) -> None:
        """
        Calculate risk overall score on security level:
            - if security specific score between 1 and 2: Leading
            - if security specific score between 2 and 4: Average
            - if security specific score above 4: Poor
            - if security specific score 0: not scored
        """
        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].calculate_risk_overall_score()

        for sov in self.portfolio_datasource.sovereigns:
            self.portfolio_datasource.sovereigns[sov].calculate_risk_overall_score()

        for muni in self.portfolio_datasource.munis:
            self.portfolio_datasource.munis[muni].calculate_risk_overall_score()

        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].calculate_risk_overall_score()

    def update_sclass(self) -> None:
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based
        """
        for c in self.portfolio_datasource.companies:
            if not self.portfolio_datasource.companies[c].isin == "NoISIN":
                self.portfolio_datasource.companies[c].update_sclass()

        for sov in self.portfolio_datasource.sovereigns:
            self.portfolio_datasource.sovereigns[sov].update_sclass()

        for muni in self.portfolio_datasource.munis:
            self.portfolio_datasource.munis[muni].update_sclass()

        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].update_sclass()

    def run(self) -> None:
        """
        run calculations
        """
        logging.log("Start Calculations")
        self.calculate_transition_score()
        self.calculate_esrm_score()
        self.calculate_securitized_score()
        self.analyst_adjustment()
        self.calculate_corporate_score()
        self.calculate_risk_overall_score()
        self.update_sclass()
