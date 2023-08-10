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


class Runner(object):
    def init(self, local_configs: str = ""):
        """
        - initialize datsources

        Parameters
        ----------
        local_configs: str, optional
            path to a local configarations file
        """

        # read params file
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
