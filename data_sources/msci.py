import requests
from base64 import b64encode
import json
from copy import deepcopy
import pandas as pd
import quantkit.utils.util_functions as util_functions
import quantkit.utils.logging as logging


class MSCI(object):
    """
    Main class to load MSCI data using the MSCI API
    When running locally, make sure to add certificate certs.crt

    Parameters
    ----------
    key: str
        msci api key
    secret: str
        msci api secret
    url: str
        msci url to get data from
    filters: dict
        dictionary of parameters for API call
    """

    def __init__(self, key: str, secret: str, url: str, filters: dict):
        self.key = key
        self.secret = secret
        self.url = url
        self.filters = filters

    def load(self):
        """
        Load data from MSCI API and save as pd.DataFrame in self.df
        """
        b64login = b64encode(
            bytes(
                "%s:%s"
                % (
                    self.key,
                    self.secret,
                ),
                encoding="utf-8",
            )
        ).decode("utf-8")

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Basic %s" % b64login,
        }

        # MSCI API can only handle 1000 issuers at once
        batches = list(
            util_functions.divide_chunks(self.filters["issuer_identifier_list"], 1000)
        )
        self.df = pd.DataFrame()
        for batch in batches:
            filters = deepcopy(self.filters)
            filters["issuer_identifier_list"] = batch
            inp = json.dumps(filters)

            response = requests.request(
                "POST", self.url, headers=headers, data=inp, verify="certs.crt"
            )
            message = response.json()["messages"]
            if message:
                logging.log(message)
            df = pd.DataFrame(response.json()["result"]["issuers"])
            self.df = pd.concat([self.df, df], ignore_index=True)
        return
