import quantkit.utils.configs as configs
import quantkit.utils.logging as logging
import quantkit.core.data_loader.regions_datasource as regions_datasource
import quantkit.core.data_loader.msci_esg_datasource as msci_esg_datasource
import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource
import quantkit.core.data_loader.security_datasource as security_datasource
import quantkit.core.data_loader.themes_datasource as themes_datasource
import quantkit.core.data_loader.category_datasource as category_database
import quantkit.core.data_loader.sector_datasource as sector_database
import quantkit.core.data_loader.transition_company_datasource as transition_company_datasource
import quantkit.core.data_loader.parent_issuer_datasource as pi_datasource
import quantkit.core.data_loader.sdg_datasource as sdg_datasource
import quantkit.core.data_loader.securitized_datasource as securitized_datasource
import quantkit.core.data_loader.exclusions_datasource as exclusions_database
import quantkit.core.data_loader.adjustment_datasource as adjustment_database
import quantkit.core.data_loader.r_and_d_datasource as r_and_d_datasource


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

        # connect security datasource
        self.security_datasource = security_datasource.SecurityDataSource(
            params=self.params["security_datasource"], api_settings=self.api_settings
        )

        # connect msci datasource
        self.msci_esg_datasource = msci_esg_datasource.MSCIESGDataSource(
            params=self.params["msci_datasource"], api_settings=self.api_settings
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

        # connect BCLASS datasource
        self.bclass_datasource = sector_database.BClassDataSource(
            params=self.params["bclass_datasource"],
            api_settings=self.api_settings,
        )

        # connect GICS datasource
        self.gics_datasource = sector_database.GICSDataSource(
            params=self.params["gics_datasource"],
            api_settings=self.api_settings,
        )

        # connect transition company datasource
        self.transition_company_datasource = (
            transition_company_datasource.TransitionCompanyDataSource(
                params=self.params["transition_company_datasource"],
                api_settings=self.api_settings,
            )
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

    def iter_securities(self) -> None:
        """
        - load security data
        - create security and company objects
        """
        self.security_datasource.load(
            securities=self.portfolio_datasource.security_keys,
            start_date=self.params["portfolio_datasource"]["start_date"],
            end_date=self.params["portfolio_datasource"]["end_date"],
        )
        self.security_datasource.iter()

    def iter_msci_esg(self) -> None:
        """
        iterate over MSCI data
        """
        issuer_ids = self.security_datasource.msci_ids

        self.params["msci_datasource"]["filters"]["issuer_identifier_list"] = issuer_ids
        self.msci_esg_datasource.load()
        self.msci_esg_datasource.iter()

    def iter_sovereigns(self) -> None:
        """
        Iterate over all sovereigns
        """
        logging.log("Iterate Sovereigns")
        for s, sov_store in self.portfolio_datasource.sovereigns.items():
            sov_store.iter()

    # def iter_securities(self) -> None:
    #     """
    #     Iterate over securities
    #     """
    #     logging.log("Iterate Securities")
    #     for sec, sec_store in self.portfolio_datasource.securities.items():
    #         sec_store.iter()

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
            comp_store.iter()
