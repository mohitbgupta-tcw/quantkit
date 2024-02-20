import quantkit.core.data_sources.snowflake as snowflake_ds
import quantkit.utils.configs as configs
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def load_from_snowflake(
    database: str = None,
    schema: str = None,
    table_name: str = None,
    query: str = None,
    local_configs: str = "",
) -> pd.DataFrame:
    """
    Load DataFrame from Snowflake
    Either the database, schema, and tablename must be provided, or a query has to be inputted.

    Parameters
    ----------
    database: str, optional
        Snowflake database
    schema: str, optional
        Snowflake schema
    table_name: str, optional
        Snowflake table
    query: str, optional
        query to be run to snowflake server
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        Snowflake DataFrame
    """

    params = configs.read_configs(local_configs=local_configs)
    snowflake_params = params["API_settings"]["snowflake_parameters"]
    sf = snowflake_ds.Snowflake(schema=schema, database=database, **snowflake_params)

    if not query:
        from_table = f"""{database}.{schema}."{table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """

    sf.load(query=query)
    return sf.df


def write_to_snowflake(
    df: pd.DataFrame,
    database: str,
    schema: str,
    table_name: str,
    local_configs: str = "",
    overwrite: bool = True,
) -> None:
    """
    (Over)Write DataFrame to Snowflake

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to be saved in Snowflake
    database: str
        Snowflake database
    schema: str
        Snowflake schema
    table_name: str
        Snowflake table
    local_configs: str, optional
        path to a local configarations file
    overwrite: bool, optional
        overwrite table, if true overwrite, if false append
    """

    params = configs.read_configs(local_configs=local_configs)
    snowflake_params = params["API_settings"]["snowflake_parameters"]

    connection_parameters = {
        "account": "tcw",
        "user": snowflake_params["user"],
        "host": "tcw.west-us-2.azure.snowflakecomputing.com",
        "password": snowflake_params["password"],
        "role": snowflake_params["role"],
        "database": database,
        "schema": schema,
    }
    conn = snowflake.connector.connect(**connection_parameters)
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name, auto_create_table=True, overwrite=overwrite
    )


def append_to_monthly_history(
    df: pd.DataFrame,
    local_configs: str = "",
) -> None:
    """
    Overwrite the monthly history DataFrame in Snowflake
    by appending newest data

    Parameters
    ----------
    df: pd.DataFrame
        Month-End DataFrame
    local_configs: str, optional
        path to a local configarations file
    """
    write_to_snowflake(
        df,
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed_History_Monthly",
        local_configs=local_configs,
        overwrite=False,
    )


def append_to_daily_history(
    df: pd.DataFrame,
    local_configs: str = "",
) -> None:
    """
    Overwrite the daily history DataFrame in Snowflake by appending newest data

    Parameters
    ----------
    df: pd.DataFrame
        Day-End DataFrame
    local_configs: str, optional
        path to a local configarations file
    """
    write_to_snowflake(
        df,
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed_History_Daily",
        local_configs=local_configs,
        overwrite=False,
    )
