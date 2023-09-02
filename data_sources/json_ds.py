import pandas as pd


class JSON(object):
    """
    Main class to load JSON related files

    Parameters
    ----------
    file: str
        file path with file extension
    orient: str, optional
        Indication of expected JSON string format
    """

    def __init__(self, json_str: str, orient: str = "index") -> None:
        self.json_str = json_str
        self.orient = orient

    def load(self) -> None:
        """
        Load file and save data as pd.DataFrame in self.df
        """
        self.df = pd.read_json(self.json_str, orient=self.orient)
