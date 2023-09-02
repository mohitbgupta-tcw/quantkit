import pandas as pd


class Category(object):
    """
    Category object.
    Saves Relevant Factor and thresholds
    as well as Flagging thresholds for Emerging and Developed Markets

    Parameters
    ----------
    name: str
        name of category
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def add_esrm_df(self, esrm_df: pd.DataFrame) -> None:
        """
        Attach ESRM df to category

        Parameters
        esrm_df: pd.DataFrame
            DataFrame with esrm information
        """
        self.esrm_df = esrm_df

    def add_DM_flags(self, dm_flags: list) -> None:
        """
        Attach flag thresholds for developed markets

        Parameters
        ----------
        dm_flags: list
            ordered list of thresholds
        """
        self.DM_flags = dm_flags

    def add_EM_flags(self, em_flags: list) -> None:
        """
        Attach flag thresholds for emerging markets

        Parameters
        ----------
        em_flags: list
            ordered list of thresholds
        """
        self.EM_flags = em_flags
