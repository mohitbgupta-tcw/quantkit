import pandas as pd


class Blank(object):
    """
    Main class in case data should not be loaded
    """

    def __init__(self):
        pass

    def load(self):
        """
        Load csv file and save data as pd.DataFrame in self.df
        """
        self.df = pd.DataFrame(columns=["ticker", "ISIN"])
