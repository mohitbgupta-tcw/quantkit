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

    def load(self) -> None:
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
                "POST", self.url, headers=headers, data=inp, verify="quantkit/certs.crt"
            )
            message = response.json()["messages"]
            if message:
                logging.log(message)
            df = pd.DataFrame(response.json()["result"]["issuers"])
            self.df = pd.concat([self.df, df], ignore_index=True)

    def load_historical(self) -> None:
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

        self.df = pd.DataFrame()
        inp = json.dumps(self.filters)

        response = requests.request(
            "POST",
            self.url,
            headers=headers,
            data=inp,
            verify="quantkit/certs.crt",
        )
        message = response.json()["messages"]
        if message:
            logging.log(message)
        batches = response.json()["result"]["response_metadata"][
            "total_number_of_batches"
        ]
        data_request_id = response.json()["result"]["response_metadata"][
            "data_request_id"
        ]

        for batch in range(batches):
            logging.log(f"Batch: {batch+1}")
            url = f"https://api2.msci.com/esg/data/v1.0/issuers/history"
            js = {"batch_id": batch + 1, "data_request_id": data_request_id}
            r_batch = requests.request(
                "POST", url, headers=headers, json=js, verify="quantkit/certs.crt"
            )
            r = r_batch.json()["result"]["data"]
            data_list = []
            for i, c in enumerate(r):
                issuer = r[i]["requested_id"]
                for j, f in enumerate(r[i]["factors"]):
                    factor_name = r[i]["factors"][j]["name"]
                    data = r[i]["factors"][j]["data_values"]
                    for k, d in enumerate(data):
                        value = data[k]["value"]
                        aod = data[k]["as_of_date"]
                        data_list.append((issuer, factor_name, value, aod))
            df = pd.DataFrame(
                data=data_list, columns=["Client_ID", "Factor", "Value", "As_Of_Date"]
            )
            self.df = pd.concat([self.df, df], ignore_index=True)

        self.df = self.df.pivot(
            index=["Client_ID", "As_Of_Date"], columns="Factor", values="Value"
        ).reset_index()
