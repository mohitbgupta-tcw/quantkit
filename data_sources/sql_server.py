import pandas as pd
import pyodbc


class SQL(object):
    """
    Main class to load data from SQL database

    Parameters
    ----------
    server: str
        database server name
    database: str
        database name
    driver: str, optional
        database driver
    trusted_connection: str, optional
        connection to database
    """

    def __init__(
        self,
        server: str,
        database: str,
        driver: str = "SQL Server",
        trusted_connection: str = "yes",
        **kwargs,
    ) -> None:
        self.driver = "{" + driver + "}"
        self.server = server
        self.database = database
        self.trusted_connection = trusted_connection
        self.connection_parameters = (
            f"""DRIVER={self.driver};"""
            + f"SERVER={self.server};"
            + f"DATABASE={self.database};"
            + f"TRUSTED_CONNECTION={self.trusted_connection}"
        )

    def load(self, query: str) -> None:
        """
        Load file and save data as pd.DataFrame in self.df

        Parameters
        ----------
        query: str
            SQL query for data
        """
        conn = pyodbc.connect(self.connection_parameters)
        self.df = pd.read_sql_query(query, conn)
