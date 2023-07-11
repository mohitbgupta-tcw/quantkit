import pandas as pd
import quantkit.utils.configs as configs
import quantkit.finance.data_sources.regions_datasource.regions_datasource as rd
import quantkit.finance.data_sources.category_datasource.category_database as cd
import quantkit.finance.data_sources.security_datasource.security_datasource as sd
import quantkit.finance.data_sources.msci_datasource.msci_datasource as mscids
import quantkit.finance.data_sources.bloomberg_datasource.bloomberg_datasource as blds
import quantkit.finance.data_sources.quandl_datasource.quandl_datasource as quds
import quantkit.finance.data_sources.portfolio_datasource.portfolio_datasource as pod
import quantkit.finance.data_sources.sdg_datasource.sdg_datasource as sdgp
import quantkit.finance.data_sources.sector_datasource.sector_database as secdb
import quantkit.finance.data_sources.exclusions_datasource.exclusions_database as exd
import quantkit.finance.data_sources.themes_datasource.themes_datasource as thd
import quantkit.finance.data_sources.transition_datasource.transition_datasource as trd
import quantkit.finance.data_sources.adjustment_datasource.adjustment_database as ads
import quantkit.finance.data_sources.securitized_datasource.securitized_datasource as securidb
import quantkit.finance.data_sources.parentissuer_datasource.pi_datasource as pis
import quantkit.utils.logging as logging


class Runner(object):
    def init(self):
        """
        - initialize datsources and load data
        - create reusable attributes
        - itereate over DataFrames and create connected objects
        """

        # read params file
        self.params = configs.read_configs()

        # connect themes datasource
        self.theme_datasource = thd.ThemeDataSource(
            self.params["theme_datasource"], self.params["theme_calculation"]
        )

        # connect regions datasource
        self.region_datasource = rd.RegionsDataSource(self.params["regions_datasource"])

        # connect portfolio datasource
        self.portfolio_datasource = pod.PortfolioDataSource(
            self.params["portfolio_datasource"]
        )

        # connect category datasource
        self.category_datasource = cd.CategoryDataSource(
            self.params["category_datasource"]
        )

        # connect sector datasource
        self.sector_datasource = secdb.SectorDataSource(
            self.params["sector_datasource"]
        )

        # connect BCLASS datasource
        self.bclass_datasource = secdb.BClassDataSource(
            self.params["bclass_datasource"], self.params["transition_parameters"]
        )

        # connect GICS datasource
        self.gics_datasource = secdb.GICSDataSource(
            self.params["gics_datasource"], self.params["transition_parameters"]
        )

        # connect transition datasource
        self.transition_datasource = trd.TransitionDataSource(
            self.params["transition_datasource"]
        )

        # connecy security datasource
        self.security_datasource = sd.SecurityDataSource(
            self.params["security_datasource"]
        )

        # connect parent issuer datasource
        self.parent_issuer_datasource = pis.ParentIssuerSource(
            self.params["parent_issuer_datasource"]
        )

        # connect SDG datasource
        self.sdg_datasource = sdgp.SDGDataSource(self.params["sdg_datasource"])

        # connect securitized mapping datasource
        self.securitized_datasource = securidb.SecuritizedDataSource(
            self.params["securitized_datasource"]
        )

        # connect exclusion datasource
        self.exclusion_datasource = exd.ExclusionsDataSource(
            self.params["exclusion_datasource"]
        )

        # connect analyst adjustment datasource
        self.adjustment_datasource = ads.AdjustmentDataSource(
            self.params["adjustment_datasource"]
        )

        # connect msci datasource
        self.msci_datasource = mscids.MSCIDataSource(self.params["msci_datasource"])

        # connect bloomberg datasource
        self.bloomberg_datasource = blds.BloombergDataSource(
            self.params["bloomberg_datasource"]
        )

        # connect quandl datasource
        self.quandl_datasource = quds.QuandlDataSource(self.params["quandl_datasource"])

        # iterate over dataframes and create objects
        logging.log("Start Iterating")
        self.iter()

    def iter(self):
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
        return

    def iter_themes(self):
        """
        - load theme data
        - create Theme objects for each theme
        """
        self.theme_datasource.load()
        self.theme_datasource.iter()
        return

    def iter_regions(self):
        """
        - load region data
        - create region objects and save in dict
        """
        self.region_datasource.load()
        self.region_datasource.iter()
        return

    def iter_sectors(self):
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
        return

    def iter_transitions(self):
        """
        - load transition data
        """
        self.transition_datasource.load()
        self.transition_datasource.iter(
            self.gics_datasource.gics, self.bclass_datasource.bclass
        )
        return

    def iter_portfolios(self):
        """
        - load portfolio data
        - create portfolio objects
        - attach Sector to Portfolio object
        """
        self.portfolio_datasource.load()
        self.portfolio_datasource.iter()

        # attach sector to portfolio
        self.sector_datasource.iter_portfolios(self.portfolio_datasource.portfolios)
        return

    def iter_securities(self):
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
        # only iterate over securities the portfolios actually hold so save time
        self.security_datasource.iter(
            securities=self.portfolio_datasource.all_holdings,
            companies=self.portfolio_datasource.companies,
            df_portfolio=self.portfolio_datasource.df,
            msci_df=self.msci_datasource.df,
            adjustment_df=self.adjustment_datasource.df,
        )

        # for sec in self.portfolio_datasource.all_holdings:
        # security_row = df_[df_["Security ISIN"] == sec]
        # # we have information for security in our database
        # if not security_row.empty:
        #     sec_info = security_row.squeeze().to_dict()
        #     if pd.isna(sec_info["ISIN"]):
        #         sec_info["ISIN"] = sec

        # # no information about security in database --> get information from portfoliot tab
        # else:
        #     portfolio_row = df_portfolio[df_portfolio["ISIN"] == sec]
        #     sec_info = security_row.reindex(list(range(1))).squeeze().to_dict()
        #     sec_info["Security ISIN"] = sec
        #     sec_info["ISIN"] = sec
        #     sec_info["ISSUERID"] = "NoISSUERID"
        #     sec_info["IssuerName"] = portfolio_row["ISSUER_NAME"].values[0]
        #     sec_info["Ticker"] = portfolio_row["Ticker Cd"].values[0]

        # # get MSCI information of parent issuer
        # issuer_id = sec_info["ISSUERID"]
        # msci_row = msci_df[msci_df["CLIENT_IDENTIFIER"] == issuer_id]
        # # issuer has msci information --> overwrite security information
        # if not msci_row.empty:
        #     issuer_isin = msci_row["ISSUER_ISIN"].values[0]
        #     if not pd.isna(issuer_isin):
        #         sec_info["ISIN"] = issuer_isin
        #         sec_info["IssuerName"] = msci_row["ISSUER_NAME"].values[0]
        #         sec_info["Ticker"] = msci_row["ISSUER_TICKER"].values[0]
        #     msci_information = msci_row.squeeze().to_dict()
        # else:
        #     msci_information = msci_row.reindex(list(range(1))).squeeze().to_dict()
        #     msci_information["ISSUER_NAME"] = sec_info["IssuerName"]
        #     msci_information["ISSUER_ISIN"] = sec_info["ISIN"]
        #     msci_information["ISSUER_TICKER"] = sec_info["Ticker"]

        # # append to all ticker list
        # self.quandl_datasource.all_tickers.append(sec_info["Ticker"])

        # # create security store --> seperate Fixed Income and Equity stores based on Security Type
        # # if Security Type is NA, just create Security object
        # sec_type = sec_info["Security Type"]
        # class_ = mapping_configs.security_store_mapping[sec_type]
        # type_mapping = mapping_configs.security_mapping[sec_type]
        # security_store = class_(isin=sec, information=sec_info)
        # self.securities[sec] = security_store
        # getattr(self, type_mapping)[sec] = self.securities[sec]

        # # create company object
        # # company object could already be there if company has more than 1 security --> get
        # issuer = sec_info["ISIN"]
        # self.portfolio_datasource.companies[
        #     issuer
        # ] = self.portfolio_datasource.companies.get(
        #     issuer,
        #     comp.CompanyStore(
        #         isin=issuer,
        #         row_data=msci_information,
        #     ),
        # )

        # # attach security to company and vice versa
        # self.portfolio_datasource.companies[issuer].add_security(
        #     sec, self.securities[sec]
        # )
        # self.securities[sec].parent_store = self.portfolio_datasource.companies[
        #     issuer
        # ]

        # # attach adjustment
        # adj_df = self.adjustment_datasource.df[
        #     self.adjustment_datasource.df["ISIN"] == sec
        # ]
        # if not adj_df.empty:
        #     self.portfolio_datasource.companies[issuer].Adjustment = pd.concat(
        #         [self.portfolio_datasource.companies[issuer].Adjustment, adj_df],
        #         ignore_index=True,
        #         sort=False,
        #     )
        return

    def iter_securitized_mapping(self):
        """
        Iterate over the securitized mapping
        """
        self.securitized_datasource.load()
        self.securitized_datasource.iter()
        return

    def iter_holdings(self):
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
        return

    def iter_sdg(self):
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
        return

    def iter_bloomberg(self):
        """
        iterate over bloomberg data
        - attach bloomberg information to company in self.bloomberg_information
        - if company doesn't have data, attach all nan's
        """
        # load bloomberg data
        self.bloomberg_datasource.load()
        self.bloomberg_datasource.iter(self.portfolio_datasource.companies)
        return

    def iter_quandl(self):
        """
        iterate over quandl data
        - attach quandl information to company in self.quandl_information
        - if company doesn't have data, attach all nan's
        """
        # load quandl data
        self.quandl_datasource.load(self.security_datasource.all_tickers)
        self.quandl_datasource.iter(self.portfolio_datasource.companies)
        return

    def iter_sovereigns(self):
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
        return

    def iter_securitized(self):
        """
        Iterate over all Securitized
        """
        logging.log("Iterate Securitized")
        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].iter(
                gics_d=self.gics_datasource.gics
            )

    def iter_muni(self):
        """
        Iterate over all Munis
        """
        logging.log("Iterate Munis")
        for m in self.portfolio_datasource.munis:
            self.portfolio_datasource.munis[m].iter(gics_d=self.gics_datasource.gics)

    def iter_companies(self):
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
        return

    def attach_parent_issuer(self):
        """
        Manually add parent issuer for selected securities
        """
        self.parent_issuer_datasource.iter(
            self.portfolio_datasource.companies, self.security_datasource.securities
        )
        return

    def replace_carbon_median(self):
        """
        For companies without 'Carbon Intensity (Scope 123)'
        --> (CARBON_EMISSIONS_SCOPE123 / SALES_USD_RECENT) couldnt be calculuated
        --> replace NA with company's industry median
        """
        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].replace_carbon_median()
        return

    def replace_transition_risk(self):
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
        return

    def calculate_securitized_score(self):
        """
        Calculation of Securitized Score
        """
        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].calculate_securitized_score()
        return

    def analyst_adjustment(self):
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
            self.portfolio_datasource.companies[c].analyst_adjustment(
                self.theme_datasource.themes
            )

        for sov in self.portfolio_datasource.sovereigns:
            self.portfolio_datasource.sovereigns[sov].analyst_adjustment(
                self.theme_datasource.themes
            )

        for muni in self.portfolio_datasource.munis:
            self.portfolio_datasource.munis[muni].analyst_adjustment(
                self.theme_datasource.themes
            )

        for sec in self.portfolio_datasource.securitized:
            self.portfolio_datasource.securitized[sec].analyst_adjustment(
                self.theme_datasource.themes
            )
        return

    def calculate_esrm_score(self):
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
        return

    def calculate_transition_score(self):
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
        return

    def calculate_corporate_score(self):
        """
        Calculate corporate score for a company based on other scores.
        Calculation:

            (Governance Score + ESRM Score + Transition Score) / 3
        """
        for c in self.portfolio_datasource.companies:
            self.portfolio_datasource.companies[c].calculate_corporate_score()
        return

    def calculate_risk_overall_score(self):
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
        return

    def update_sclass(self):
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

    def run(self):
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
        return
