import pandas as pd


class Microsoft(object):
    """
    Main class to load Microsoft related files, such as .xlsx and .csv

    Parameters
    ----------
    file: str
        file path with file extension
    """

    def __init__(self, file: str):
        self.file = file

    def load(self):
        """
        Load file and save data as pd.DataFrame in self.df
        """
        raise NotImplementedError


class CSV(Microsoft):
    """
    Main class to load CSV files with extension .csv

    Parameters
    ----------
    file: str
        file path with file extension
    """

    def __init__(self, file):
        super().__init__(file)

    def load(self):
        """
        Load csv file and save data as pd.DataFrame in self.df
        """
        self.df = pd.read_csv(self.file)


class Excel(Microsoft):
    """
    Main class to load Excel files with extension .xslx

    Parameters
    ----------
    file: str
        file path with file extension
    sheet_name: str
        sheet name of Excel file
    engine: str, optional
        engine to load Excel file with, default: openpyxl
    """

    def __init__(self, file: str, sheet_name: str, engine: str = "openpyxl"):
        self.sheet_name = sheet_name
        self.engine = engine
        super().__init__(file)

    def load(self):
        """
        Load Excel file and save data as pd.DataFrame in self.df
        """
        na_list = [
            "-1.#IND",
            "1.#QNAN",
            "1.#IND",
            "-1.#QNAN",
            "#N/A N/A",
            "#N/A",
            "N/A",
            "n/a",
            "#NA",
            "NULL",
            "null",
            "NaN",
            "-NaN",
            "nan",
            "-nan",
            "",
            "None",
        ]
        self.df = pd.read_excel(
            self.file,
            sheet_name=self.sheet_name,
            engine=self.engine,
            na_values=na_list,
            keep_default_na=False,
        )
