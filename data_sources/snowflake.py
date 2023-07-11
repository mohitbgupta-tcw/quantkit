# from snowflake.snowpark.session import Session
import pandas as pd


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
    table_name: str
        snowflake table name
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
        table_name: str,
        account: str = "tcw",
        host: str = "tcw.west-us-2.azure.snowflakecomputing.com",
    ):
        self.account = account
        self.user = user
        self.host = host
        self.password = password
        self.role = role
        self.database = database
        self.schema = schema
        self.connection_parameters = {
            "account": self.account,
            "user": self.user,
            "host": self.host,
            "password": self.password,
            "role": self.role,
            "database": self.database,
            "schema": self.schema,
        }
        self.table_name = table_name

    def load(self):
        """
        Load data from snowflake and save as pd.DataFrame in self.df
        """
        session = Session.builder.configs(self.connection_parameters).create()

        from_table = f"{self.database}.{self.schema}.{self.table_name}"
        query = f"""
        SELECT * 
        FROM {from_table}
        """

        df_table = session.sql(query)
        self.df = pd.DataFrame(df_table.collect())
        return
