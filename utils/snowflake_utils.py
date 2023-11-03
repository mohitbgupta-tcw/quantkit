import quantkit.data_sources.snowflake as snowflake
import quantkit.utils.configs as configs
import pandas as pd
from snowflake.snowpark.session import Session


def load_from_snowflake(
    database: str,
    schema: str,
    table_name: str,
    query: str = None,
    local_configs: str = "",
) -> pd.DataFrame:
    """
    Load DataFrame from Snowflake

    Parameters
    ----------
    database: str
        Snowflake database
    schema: str
        Snowflake schema
    table_name: str
        Snowflake table
    local_configs: str, optional
        path to a local configarations file
    query: str, optional
        query to be run to snowflake server

    Returns
    -------
    pd.DataFrame
        Snowflake DataFrame
    """

    params = configs.read_configs(local_configs=local_configs)
    snowflake_params = params["API_settings"]["snowflake_parameters"]
    sf = snowflake.Snowflake(schema=schema, database=database, **snowflake_params)

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
) -> None:
    """
    Write DataFrame to Snowflake

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

    session = Session.builder.configs(connection_parameters).create()
    session.write_pandas(
        df, table_name=table_name, auto_create_table=True, overwrite=True
    )


def overwrite_history(
    local_configs: str = "",
) -> None:
    """
    Overwrite the history DataFrame in Snowflake by appending newest data

    Parameters
    ----------
    local_configs: str, optional
        path to a local configarations file
    """
    df_new = load_from_snowflake(
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed",
        local_configs=local_configs,
    )
    df_history = load_from_snowflake(
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed_History_Monthly",
        local_configs=local_configs,
    )
    df_history = pd.concat([df_history, df_new])
    df_history = df_history.sort_values(
        ["As Of Date", "Portfolio ISIN", "Portfolio Weight"],
        ascending=[True, True, False],
    )

    write_to_snowflake(
        df_history,
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed_History_Monthly",
        local_configs=local_configs,
    )


def overwrite_daily(
    local_configs: str = "",
) -> None:
    """
    Overwrite the daily history DataFrame in Snowflake by appending newest data
    keep track of the last 60 trading days

    Parameters
    ----------
    local_configs: str, optional
        path to a local configarations file
    """
    df_new = load_from_snowflake(
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed",
        local_configs=local_configs,
    )
    df_history = load_from_snowflake(
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed_History_Daily",
        local_configs=local_configs,
    )
    df_history = pd.concat([df_history, df_new])
    dates = sorted(list(df_history["As Of Date"].unique()), reverse=True)
    dates = dates[:60]

    df_history = df_history[df_history["As Of Date"].isin(dates)]
    df_history = df_history.sort_values(
        ["As Of Date", "Portfolio ISIN", "Portfolio Weight"],
        ascending=[True, True, False],
    )

    write_to_snowflake(
        df_history,
        database="SANDBOX_ESG",
        schema="ESG_SCORES_THEMES",
        table_name="Sustainability_Framework_Detailed_History_Daily",
        local_configs=local_configs,
    )
