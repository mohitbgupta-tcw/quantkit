import quantkit.utils.configs as configs
import quantkit.utils.logging as logging
import quantkit.finance.data_sources.regions_datasource as regions_datasource
import quantkit.finance.data_sources.category_datasource as category_database
import quantkit.finance.data_sources.msci_datasource as msci_datasource
import quantkit.finance.data_sources.bloomberg_datasource as bloomberg_datasource
import quantkit.finance.data_sources.prices_datasource as prices_datasource
import quantkit.finance.data_sources.fundamentals_datasource as fundamentals_datasource
import quantkit.finance.data_sources.marketmultiple_datasource as marketmultiple_datasource
import quantkit.finance.data_sources.portfolio_datasource as portfolio_datasource
import quantkit.finance.data_sources.sdg_datasource as sdg_datasource
import quantkit.finance.data_sources.sector_datasource as sector_database
import quantkit.finance.data_sources.exclusions_datasource as exclusions_database
import quantkit.finance.data_sources.themes_datasource as themes_datasource
import quantkit.finance.data_sources.transition_datasource as transition_datasourced
import quantkit.finance.data_sources.adjustment_datasource as adjustment_database
import quantkit.finance.data_sources.securitized_datasource as securitized_datasource
import quantkit.finance.data_sources.parent_issuer_datasource as pi_datasource
import quantkit.finance.data_sources.factor_datasource as factor_datasource
from copy import deepcopy


class Runner(object):
    def init(self, local_configs: str = "", runner_type: str = None) -> None:
        """
        initialize datsources

        Parameters
        ----------
        local_configs: str, optional
            path to a local configarations file
        runner_type: str, optional
            runner type to include configs for, p.e. "risk_framework", "asset_allocation", "pai"
        """

        # read params file
        self.local_configs = local_configs
        self.params = configs.read_configs(local_configs, runner_type)
        self.api_settings = self.params["API_settings"]

        # connect themes datasource
        theme_calculations = self.params.get("theme_calculation", dict())
        self.theme_datasource = themes_datasource.ThemeDataSource(
            params=self.params["theme_datasource"],
            theme_calculations=theme_calculations,
            api_settings=self.api_settings,
        )

        # connect regions datasource
        self.region_datasource = regions_datasource.RegionsDataSource(
            params=self.params["regions_datasource"], api_settings=self.api_settings
        )

        # connect portfolio datasource
        self.portfolio_datasource = portfolio_datasource.PortfolioDataSource(
            params=self.params["portfolio_datasource"], api_settings=self.api_settings
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
        self.transition_datasource = transition_datasourced.TransitionDataSource(
            params=self.params["transition_datasource"], api_settings=self.api_settings
        )

        # connect parent issuer datasource
        self.parent_issuer_datasource = pi_datasource.ParentIssuerSource(
            params=self.params["parent_issuer_datasource"],
            api_settings=self.api_settings,
        )
        self.ticker_parent_issuer_datasource = pi_datasource.TickerParentIssuerSource(
            params=self.params["ticker_parent_issuer_datasource"],
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

        # connect msci datasource
        self.msci_datasource = msci_datasource.MSCIDataSource(
            params=self.params["msci_datasource"], api_settings=self.api_settings
        )

        # connect bloomberg datasource
        self.bloomberg_datasource = bloomberg_datasource.BloombergDataSource(
            params=self.params["bloomberg_datasource"], api_settings=self.api_settings
        )

        # connect fundamental and price datasource
        self.fundamentals_datasource = fundamentals_datasource.FundamentalsDataSource(
            params=self.params["fundamentals_datasource"],
            api_settings=self.api_settings,
        )
        self.prices_datasource = prices_datasource.PricesDataSource(
            params=self.params["prices_datasource"],
            api_settings=self.api_settings,
        )
        self.marketmultiple_datasource = (
            marketmultiple_datasource.MarketMultipleDataSource(
                params=self.params["marketmultiple_datasource"],
                api_settings=self.api_settings,
            )
        )

        # connect factors datasource
        self.factor_datasource = factor_datasource.FactorsDataSource(
            params=self.params["factor_datasource"],
            api_settings=self.api_settings,
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

    def iter_portfolios(self) -> None:
        """
        - load portfolio data
        - create portfolio objects
        """
        self.portfolio_datasource.load(
            start_date=self.params["as_of_date"],
            end_date=self.params["as_of_date"],
            pfs=self.params["portfolios"],
            equity_benchmark=self.params["equity_benchmark"],
            fixed_income_benchmark=self.params["fixed_income_benchmark"],
        )
        self.portfolio_datasource.iter()

    def iter_securitized_mapping(self) -> None:
        """
        Iterate over the securitized mapping
        """
        self.securitized_datasource.load()
        self.securitized_datasource.iter()

    def iter_holdings(self) -> None:
        """
        Iterate over portfolio holdings
        - Create Security objects
        - create Company, Muni, Sovereign, Securitized, Cash objects
        - attach holdings, OAS to self.holdings with security object
        """
        self.portfolio_datasource.iter_holdings(
            msci_dict=self.msci_datasource.msci,
        )

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
        self.ticker_parent_issuer_datasource.load()
        self.ticker_parent_issuer_datasource.iter()

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
        # load SDG data
        self.sdg_datasource.load(
            as_of_date=self.params["as_of_date"],
        )
        self.sdg_datasource.iter()

    def iter_bloomberg(self) -> None:
        """
        iterate over bloomberg data
        """
        # load bloomberg data
        self.bloomberg_datasource.load()
        self.bloomberg_datasource.iter()

    def iter_prices(self) -> None:
        """
        iterate over price data
        """
        self.params["prices_datasource"]["filters"][
            "ticker"
        ] = self.portfolio_datasource.all_tickers
        self.prices_datasource.load(
            **self.params["prices_datasource"]["filters"]["date"]
        )
        self.prices_datasource.iter(self.portfolio_datasource.all_tickers)

    def iter_fundamentals(self) -> None:
        """
        iterate over fundamental data
        """
        # load parent issuer data
        parent_tickers = self.ticker_parent_issuer_datasource.tickers()
        tickers = deepcopy(self.portfolio_datasource.all_tickers)
        tickers += parent_tickers
        tickers = list(set(tickers))

        # load fundamental data
        self.params["fundamentals_datasource"]["filters"]["ticker"] = tickers
        self.params["fundamentals_datasource"][
            "duplication"
        ] = self.ticker_parent_issuer_datasource.parent_issuers
        self.fundamentals_datasource.load(
            **self.params["fundamentals_datasource"]["filters"]["calendardate"]
        )
        self.fundamentals_datasource.iter(self.portfolio_datasource.all_tickers)

    def iter_marketmuliples(self) -> None:
        """
        iterate over market multiple data
        """
        self.marketmultiple_datasource.load()
        self.marketmultiple_datasource.iter()

    def iter_factors(self) -> None:
        """
        iterate over factor data
        """
        self.factor_datasource.load()
        self.factor_datasource.iter()

    def iter_securities(self) -> None:
        """
        - Overwrite Parent
        - Add ESG Collateral Type
        - Attach BClass Level4 to parent
        - Attach Sector Level 2
        - Attach Analyst Adjustment
        - Attach Bloomberg information
        - Attach ISS information
        - Attach Fundamental information
        - Attach Price information
        """
        logging.log("Iterate Securities")
        for sec, sec_store in self.portfolio_datasource.securities.items():
            sec_store.iter(
                parent_issuer_dict=self.parent_issuer_datasource.parent_issuers,
                companies=self.portfolio_datasource.companies,
                securitized_mapping=self.securitized_datasource.securitized_mapping,
                bclass_dict=self.bclass_datasource.bclass,
                sec_adjustment_dict=self.adjustment_datasource.security_isins,
                bloomberg_dict=self.bloomberg_datasource.bloomberg,
                sdg_dict=self.sdg_datasource.sdg,
                dict_fundamental=self.fundamentals_datasource.tickers,
                dict_prices=self.prices_datasource.tickers,
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
