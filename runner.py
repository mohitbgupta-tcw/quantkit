import quantkit.utils.configs as configs
import quantkit.utils.logging as logging
import quantkit.core.data_loader.regions_datasource as regions_datasource
import quantkit.core.data_loader.msci_datasource as msci_datasource
import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource


class Runner(object):
    def init(self, local_configs: str = "", runner_type: str = None) -> None:
        """
        initialize datsources

        Parameters
        ----------
        local_configs: str, optional
            path to a local configarations file
        runner_type: str, optional
            runner type to include configs for, p.e. "risk_framework", "backtester", "pai"
        """

        # read params file
        self.local_configs = local_configs
        self.params = configs.read_configs(local_configs, runner_type)
        self.api_settings = self.params["API_settings"]

        # connect regions datasource
        self.region_datasource = regions_datasource.RegionsDataSource(
            params=self.params["regions_datasource"], api_settings=self.api_settings
        )

        # connect portfolio datasource
        self.portfolio_datasource = portfolio_datasource.PortfolioDataSource(
            params=self.params["portfolio_datasource"], api_settings=self.api_settings
        )

        # connect msci datasource
        self.msci_datasource = msci_datasource.MSCIDataSource(
            params=self.params["msci_datasource"], api_settings=self.api_settings
        )

    def iter_regions(self) -> None:
        """
        - load region data
        - create region objects and save in dict
        """
        self.region_datasource.load()
        self.region_datasource.iter()

    def iter_portfolios(self) -> None:
        """
        - load portfolio data
        - create portfolio objects
        """
        self.portfolio_datasource.load(
            start_date=self.params["portfolio_datasource"]["start_date"],
            end_date=self.params["portfolio_datasource"]["end_date"],
            tcw_universe=self.params["portfolio_datasource"]["tcw_universe"],
            equity_universe=self.params["portfolio_datasource"]["equity_universe"],
            fixed_income_universe=self.params["portfolio_datasource"][
                "fixed_income_universe"
            ],
        )
        self.portfolio_datasource.iter()

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

    def iter_msci(self) -> None:
        """
        iterate over MSCI data
        """
        issuer_ids = self.portfolio_datasource.all_msci_ids

        self.params["msci_datasource"]["filters"]["issuer_identifier_list"] = issuer_ids
        self.msci_datasource.load()
        self.msci_datasource.iter()

    def iter_sovereigns(self) -> None:
        """
        Iterate over all sovereigns
        """
        logging.log("Iterate Sovereigns")
        for s, sov_store in self.portfolio_datasource.sovereigns.items():
            sov_store.iter()

    def iter_securities(self) -> None:
        """
        Iterate over securities
        """
        logging.log("Iterate Securities")
        for sec, sec_store in self.portfolio_datasource.securities.items():
            sec_store.iter()

    def iter_securitized(self) -> None:
        """
        Iterate over all Securitized
        """
        logging.log("Iterate Securitized")
        for sec, sec_store in self.portfolio_datasource.securitized.items():
            sec_store.iter()

    def iter_muni(self) -> None:
        """
        Iterate over all Munis
        """
        logging.log("Iterate Munis")
        for m, muni_store in self.portfolio_datasource.munis.items():
            muni_store.iter()

    def iter_cash(self) -> None:
        """
        Iterate over all Cash objects
        """
        logging.log("Iterate Cash")
        for c, cash_store in self.portfolio_datasource.cash.items():
            cash_store.iter()

    def iter_companies(self) -> None:
        """
        Iterate over all companies
        """
        logging.log("Iterate Companies")
        for c, comp_store in self.portfolio_datasource.companies.items():
            comp_store.iter(
                companies=self.portfolio_datasource.companies,
            )
