import quantkit.data_sources.blank as blank
import quantkit.data_sources.excel as ds_excel
import quantkit.data_sources.snowflake as snowflake
import quantkit.data_sources.msci as msci
import quantkit.data_sources.quandl as quandl
import quantkit.data_sources.json_ds as json_ds
import quantkit.data_sources.sql_server as sql_server
import quantkit.data_sources.fred as fred
import quantkit.utils.configs as configs


class DataSources(object):
    """
    Main Class for data sources
    Call different data loader based on source

    Parameters
    ----------
        params: dict
            data specific parameters including source
        api_settings: dict, optional
            dictionary of api settings
    """

    def __init__(self, params: dict, api_settings: dict = None, **kwargs) -> None:
        self.params = params

        # ignore datasource if load is False
        if not params["load"]:
            self.datasource = blank.Blank()

        # Excel
        elif params["source"] == 1:
            self.file = params["file"]
            self.datasource = ds_excel.Excel(self.file, sheet_name=params["sheet_name"])

        # CSV
        elif params["source"] == 2:
            self.file = params["file"]
            self.datasource = ds_excel.CSV(self.file)

        # Snowflake
        elif params["source"] == 3:
            snowflake_params = api_settings["snowflake_parameters"]
            self.datasource = snowflake.Snowflake(
                table_name=params["table_name"], **snowflake_params
            )

        # MSCI API
        elif params["source"] == 4:
            msci_params = api_settings["msci_parameters"]
            self.datasource = msci.MSCI(
                url=params["url"], filters=params["filters"], **msci_params
            )

        # Quandl API
        elif params["source"] == 5:
            quandl_params = api_settings["quandl_parameters"]
            self.datasource = quandl.Quandl(
                table=params["table"], filters=params["filters"], **quandl_params
            )

        # JSON
        elif params["source"] == 6:
            self.datasource = json_ds.JSON(json_str=params["json_str"])

        # SQL Server
        elif params["source"] == 7:
            self.datasource = sql_server.SQL()

        # FRED
        elif params["source"] == 8:
            fred_params = api_settings["fred_parameters"]
            self.datasource = fred.FRED(**fred_params)

    def transform_df(self) -> None:
        """
        Transformations to DataFrame
        """
        raise NotImplementedError
