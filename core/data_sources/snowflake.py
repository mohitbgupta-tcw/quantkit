import snowflake.connector
import pandas as pd
import quantkit.utils.snowflake_utils as snowflake_utils


class Snowflake(object):
    """
    Main class to load data from snowflake

    Parameters
    ----------
    user: str
        snowflake user name (tcw email)
    password: str
        snowflake password (receive private key from Matthew Cen)
    role: str
        snowflake role
    database: str
        snowflake database
    schema: str
        snowflake schema
    account: str, optional
        snowflake account
    host: str, optional
        snowflake host

    """

    def __init__(
        self,
        user: str,
        password: str,
        role: str,
        database: str,
        schema: str,
        airflow_connection_id: str = None,
        account: str = "tcw",
        host: str = "tcw.west-us-2.azure.snowflakecomputing.com",
        **kwargs,
    ) -> None:
        self.account = account
        self.user = user
        self.host = host
        self.password = password
        self.role = role
        self.database = database
        self.schema = schema
        self.airflow_connection_id = airflow_connection_id
        self.connection_parameters = {
            "account": self.account,
            "user": self.user,
            "host": self.host,
            "password": self.password,
            "role": self.role,
            "database": self.database,
            "schema": self.schema,
        }

    def load(self, query: str, **kwargs) -> None:
        """
        Load data from snowflake and save as pd.DataFrame in self.df

        Parameters
        ----------
        query: str
            SQL query for data
        """

        if self.airflow_connection_id != None:

            self.connection_parameters = snowflake_utils.get_snowflake_connparams(self.airflow_connection_id.
                                                         self.role,
                                                         self.schema)

        conn = snowflake.connector.connect(**self.connection_parameters)
        cur = conn.cursor()
        self.df = cur.execute(query).fetch_pandas_all()
