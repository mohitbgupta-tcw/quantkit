import pandas as pd
import quantkit.utils.logging as logging
from pathlib import Path

class Microsoft(object):
    """
    Main class to load Microsoft related files, such as .xlsx and .csv

    Parameters
    ----------
    file: str
        file path with file extension
    """

    def __init__(self, file: str, **kwargs) -> None:
        self.file = file

    def load(self, **kwargs) -> None:
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

    def __init__(self, file, **kwargs) -> None:
        super().__init__(file)

    def load(self, **kwargs) -> None:
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

    def __init__(
        self, file: str, sheet_name: str, engine: str = "openpyxl", **kwargs
    ) -> None:
        self.sheet_name = sheet_name
        self.engine = engine
        super().__init__(file)

    def load(self, **kwargs) -> None:
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
        #logging.log('self.params********************555555555555555**********************')
        #logging.log( str(Path(__file__).resolve().parent.parent.parent) )
        #logging.log('self.params******************6666666666666666************************')
        self.df = pd.read_excel(
            str(Path(__file__).resolve().parent.parent.parent) + self.file,
            sheet_name=self.sheet_name,
            engine=self.engine,
            na_values=na_list,
            keep_default_na=False,
        )
