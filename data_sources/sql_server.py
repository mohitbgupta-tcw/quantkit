import pandas as pd
import pyodbc


class SQL(object):
    """
    Main class to load JSON related files

    Parameters
    ----------
    file: str
        file path with file extension
    orient: str, optional
        Indication of expected JSON string format
    """

    def __init__(
        self,
        server: str,
        database: str,
        driver: str = "SQL Server",
        trusted_connection: str = "yes",
    ):
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

    def load(self) -> None:
        """
        Load file and save data as pd.DataFrame in self.df
        """
        conn = pyodbc.connect(self.connection_parameters)
        query = """
            SELECT a.id,a.name, b.Cusip__c, b.ISIN__c, a.* 
            FROM ticker__c a
            LEFT JOIN [SFIntegration].[dbo].[SecurityMaster__c] b
            ON a.id = b.Ticker__c
            WHERE a.name like '%aapl%'
            ORDER BY a.name
        """
        self.df = pd.read_sql_query(query, conn)
