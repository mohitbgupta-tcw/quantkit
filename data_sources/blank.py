import pandas as pd


class Blank(object):
    """
    Main class in case data should not be loaded
    """

    def __init__(self) -> None:
        pass

    def load(self) -> None:
        """
        Load csv file and save data as pd.DataFrame in self.df
        """
        self.df = pd.DataFrame(columns=["ticker", "ISIN"])
