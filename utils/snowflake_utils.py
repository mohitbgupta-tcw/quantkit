import quantkit.data_sources.snowflake as snowflake
import quantkit.utils.configs as configs
import pandas as pd
from snowflake.snowpark.session import Session


def load_from_snowflake(
    schema: str, table_name: str, local_configs: str = ""
) -> pd.DataFrame:
    """
    Load DataFrame from Snowflake

    Parameters
    ----------
    schema: str
        Snowflake schema
    table_name: str
        Snowflake table
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        Snowflake DataFrame
    """

    params = configs.read_configs(local_configs=local_configs)
    snowflake_params = params["API_settings"]["snowflake_parameters"]
    snowflake_params["schema"] = schema
    sf = snowflake.Snowflake(table_name=table_name, **snowflake_params)
    sf.load()
    return sf.df


def write_to_snowflake(
    df: pd.DataFrame, schema: str, table_name: str, local_configs: str = ""
) -> None:
    """
    Write DataFrame to Snowflake

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to be saved in Snowflake
    schema: str
        Snowflake schema
    table_name: str
        Snowflake table
    local_configs: str, optional
        path to a local configarations file
    """

    params = configs.read_configs(local_configs=local_configs)
    snowflake_params = params["API_settings"]["snowflake_parameters"]

    connection_parameters = {
        "account": "tcw",
        "user": snowflake_params["user"],
        "host": "tcw.west-us-2.azure.snowflakecomputing.com",
        "password": snowflake_params["password"],
        "role": snowflake_params["role"],
        "database": snowflake_params["database"],
        "schema": schema,
    }

    session = Session.builder.configs(connection_parameters).create()
    session.write_pandas(
        df, table_name=table_name, auto_create_table=True, overwrite=True
    )
