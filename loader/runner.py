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
    def init(self):
        """
        - initialize datsources
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
