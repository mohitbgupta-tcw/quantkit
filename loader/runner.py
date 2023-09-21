import quantkit.utils.configs as configs
import quantkit.utils.logging as logging
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


class Runner(object):
    def init(self, local_configs: str = "") -> None:
        """
        initialize datsources

        Parameters
        ----------
        local_configs: str, optional
            path to a local configarations file
        """

        # read params file
        self.local_configs = local_configs
        self.params = configs.read_configs(local_configs)
        api_settings = self.params["API_settings"]

        # connect themes datasource
        self.theme_datasource = thd.ThemeDataSource(
            params=self.params["theme_datasource"],
            theme_calculations=self.params["theme_calculation"],
            api_settings=api_settings,
        )

        # connect regions datasource
        self.region_datasource = rd.RegionsDataSource(
            params=self.params["regions_datasource"], api_settings=api_settings
        )

        # connect portfolio datasource
        self.portfolio_datasource = pod.PortfolioDataSource(
            params=self.params["portfolio_datasource"], api_settings=api_settings
        )

        # connect category datasource
        self.category_datasource = cd.CategoryDataSource(
            params=self.params["category_datasource"], api_settings=api_settings
        )

        # connect sector datasource
        self.sector_datasource = secdb.SectorDataSource(
            params=self.params["sector_datasource"], api_settings=api_settings
        )

        # connect BCLASS datasource
        self.bclass_datasource = secdb.BClassDataSource(
            params=self.params["bclass_datasource"],
            transition_params=self.params["transition_parameters"],
            api_settings=api_settings,
        )

        # connect GICS datasource
        self.gics_datasource = secdb.GICSDataSource(
            params=self.params["gics_datasource"],
            transition_params=self.params["transition_parameters"],
            api_settings=api_settings,
        )

        # connect transition datasource
        self.transition_datasource = trd.TransitionDataSource(
            params=self.params["transition_datasource"], api_settings=api_settings
        )

        # connecy security datasource
        self.security_datasource = sd.SecurityDataSource(
            params=self.params["security_datasource"], api_settings=api_settings
        )

        # connect parent issuer datasource
        self.parent_issuer_datasource = pis.ParentIssuerSource(
            params=self.params["parent_issuer_datasource"], api_settings=api_settings
        )

        # connect SDG datasource
        self.sdg_datasource = sdgp.SDGDataSource(
            params=self.params["sdg_datasource"], api_settings=api_settings
        )

        # connect securitized mapping datasource
        self.securitized_datasource = securidb.SecuritizedDataSource(
            params=self.params["securitized_datasource"], api_settings=api_settings
        )

        # connect exclusion datasource
        self.exclusion_datasource = exd.ExclusionsDataSource(
            params=self.params["exclusion_datasource"], api_settings=api_settings
        )

        # connect analyst adjustment datasource
        self.adjustment_datasource = ads.AdjustmentDataSource(
            params=self.params["adjustment_datasource"], api_settings=api_settings
        )

        # connect msci datasource
        self.msci_datasource = mscids.MSCIDataSource(
            params=self.params["msci_datasource"], api_settings=api_settings
        )

        # connect bloomberg datasource
        self.bloomberg_datasource = blds.BloombergDataSource(
            params=self.params["bloomberg_datasource"], api_settings=api_settings
        )

        # connect quandl datasource
        self.quandl_datasource = quds.QuandlDataSource(
            params=self.params["quandl_datasource"], api_settings=api_settings
        )

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
        self.portfolio_datasource.load(
            as_of_date=self.params["as_of_date"],
            pfs=self.params["portfolios"],
            equity_benchmark=self.params["equity_benchmark"],
            fixed_income_benchmark=self.params["fixed_income_benchmark"],
        )
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
        issuer_ids = self.portfolio_datasource.all_msci_ids

        issuer_ids += parent_ids
        issuer_ids = list(set(issuer_ids))
        self.params["msci_datasource"]["filters"]["issuer_identifier_list"] = issuer_ids
        self.msci_datasource.load()
        self.msci_datasource.iter()

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
        - attach Bloomberg information
        - attach holdings, OAS to self.holdings with security object
        """
        self.portfolio_datasource.iter_holdings(
            securities=self.security_datasource.securities,
            securitized_mapping=self.securitized_datasource.securitized_mapping,
            bclass_dict=self.bclass_datasource.bclass,
            bloomberg_dict=self.bloomberg_datasource.bloomberg,
            sdg_dict=self.sdg_datasource.sdg,
        )

    def iter_sdg(self) -> None:
        """
        iterate over SDG data
        - attach sdg information to company in self.sdg_information
        - if company doesn't have data, attach all nan's
        """
        # load SDG data
        self.sdg_datasource.load()
        self.sdg_datasource.iter()

    def iter_bloomberg(self) -> None:
        """
        iterate over bloomberg data
        - attach bloomberg information to company in self.bloomberg_information
        - if company doesn't have data, attach all nan's
        """
        # load bloomberg data
        self.bloomberg_datasource.load()
        self.bloomberg_datasource.iter()

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
                bclass_d=self.bclass_datasource.bclass,
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
                bclass_d=self.bclass_datasource.bclass,
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
                bclass_d=self.bclass_datasource.bclass,
            )

    def iter_companies(self) -> None:
        """
        Iterate over all companies
        """
        logging.log("Iterate Companies")

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

    def attach_parent_issuer(self) -> None:
        """
        Manually add parent issuer for selected securities
        """
        self.parent_issuer_datasource.iter(
            self.portfolio_datasource.companies, self.security_datasource.securities
        )
