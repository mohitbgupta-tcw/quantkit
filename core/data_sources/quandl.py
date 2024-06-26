import os
import pandas as pd
from copy import deepcopy
import nasdaqdatalink
import quantkit.utils.util_functions as util_functions
import quantkit.utils.logging as logging
from pathlib import Path


class Quandl(object):
    """
    Main class to load Quandl data using the Quandl API

    Parameters
    ----------
    key: str
        quandl api key
    datatable_code: str
        datatable code
    filters: dict
        dictionary of parameters for function call
    """

    def __init__(
        self, key: str, type: str, datatable_code: str, filters: dict, **kwargs
    ) -> None:
        self.key = key
        self.type = type
        self.datatable_code = datatable_code
        self.filters = filters

    def load(self, **kwargs) -> None:
        """
        Load data from quandl API and save as pd.DataFrame in self.df
        """
        d = Path(__file__).resolve().parent.parent.parent
        nasdaqdatalink.ApiConfig.api_key = self.key
        if os.name == "nt":
            nasdaqdatalink.ApiConfig.verify_ssl = f"{d}\\certs.crt"

        if self.type in ["fundamental", "prices"]:
            if "ticker" in self.filters:
                batches = list(
                    util_functions.divide_chunks(self.filters["ticker"], 100)
                )
                self.df = pd.DataFrame()
                for i, batch in enumerate(batches):
                    logging.log(f"Batch {i+1}/{len(batches)}")
                    filters = deepcopy(self.filters)
                    filters["ticker"] = list(batch)

                    df = nasdaqdatalink.get_table(self.datatable_code, **filters)
                    self.df = pd.concat([self.df, df], ignore_index=True)
            else:
                self.df = nasdaqdatalink.get_table(self.datatable_code, **self.filters)
        elif self.type in ["market"]:
            self.df = nasdaqdatalink.get(self.datatable_code, **self.filters)
