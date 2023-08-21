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
        self.iter_securitized_mapping()
        self.iter_portfolios()
        self.iter_securities()
        self.iter_holdings()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()

    def iter_companies(self) -> None:
        """
        Iterate over all companies
        """
        logging.log("Iterate Companies")

        super().iter_companies()

        self.replace_carbon_median()
        self.replace_transition_risk()

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
