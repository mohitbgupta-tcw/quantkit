import quantkit.data_sources.excel as ds_excel
import quantkit.data_sources.snowflake as snowflake
import quantkit.data_sources.msci as msci
import quantkit.data_sources.quandl as quandl
import quantkit.data_sources.json as json
import quantkit.utils.configs as configs


class DataSources(object):
    """
    Main Class for data sources
    Call different data loader based on source

    Parameters
    ----------
        params: dict
            data specific parameters including source
    """

    all_params = configs.read_configs()

    def __init__(self, params: dict):
        self.params = params

        # Excel
        if params["source"] == 1:
            self.file = params["file"]
            self.datasource = ds_excel.Excel(self.file, sheet_name=params["sheet_name"])

        # CSV
        elif params["source"] == 2:
            self.file = params["file"]
            self.datasource = ds_excel.CSV(self.file)

        # Snowflake
        elif params["source"] == 3:
            snowflake_params = self.all_params["snowflake_parameters"]
            self.datasource = snowflake.Snowflake(
                table_name=params["table_name"], **snowflake_params
            )

        # MSCI API
        elif params["source"] == 4:
            msci_params = self.all_params["msci_parameters"]
            self.datasource = msci.MSCI(
                url=params["url"], filters=params["filters"], **msci_params
            )

        # Quand; API
        elif params["source"] == 5:
            quandl_params = self.all_params["quandl_parameters"]
            self.datasource = quandl.Quandl(
                table=params["table"], filters=params["filters"], **quandl_params
            )

        # JSON
        elif params["source"] == 6:
            self.datasource = json.JSON(json_str=params["json_str"])

    def transform_df(self):
        """
        Transformations to DataFrame
        """
        raise NotImplementedError
