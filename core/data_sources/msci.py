import os
import requests
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

    def __init__(
        self, key: str, secret: str, url: str, filters: dict, **kwargs
    ) -> None:
        self.key = key
        self.secret = secret
        self.url = url
        self.filters = filters

    def request_authorization(self) -> str:
        """
        Generate authorization token

        Returns
        -------
        str:
            authorization token
        """
        grant_type = "client_credentials"
        token_dict = {
            "grant_type": grant_type,
            "client_id": self.key,
            "client_secret": self.secret,
            "audience": "https://esg/data",
        }
        oauth_url = "https://accounts.msci.com/oauth/token/"
        if os.name == "posix":
            auth_response = requests.post(
                oauth_url,
                data=token_dict,
            )
        elif os.name == "nt":
            auth_response = requests.post(
                oauth_url, data=token_dict, verify="quantkit/certs.crt"
            )
        else:
            auth_response = requests.post(
                oauth_url, data=token_dict, verify="quantkit/certs.crt"
            )
        auth_response_json = auth_response.json()
        auth_token = auth_response_json["access_token"]
        return auth_token

    def load(self, **kwargs) -> None:
        """
        Load data from MSCI API and save as pd.DataFrame in self.df
        """

        # generate token
        auth_token = self.request_authorization()

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer %s" % auth_token,
        }

        # MSCI API can only handle 1000 issuers at once
        batches = list(
            util_functions.divide_chunks(self.filters["issuer_identifier_list"], 1000)
        )
        self.df = pd.DataFrame()
        for i, batch in enumerate(batches):
            logging.log(f"Batch {i+1}/{len(batches)}")
            filters = deepcopy(self.filters)
            filters["issuer_identifier_list"] = batch
            inp = json.dumps(filters)

            if os.name == "posix":
                response = requests.request("POST", self.url, headers=headers, data=inp)
            elif os.name == "nt":
                response = requests.request(
                    "POST",
                    self.url,
                    headers=headers,
                    data=inp,
                    verify="quantkit/certs.crt",
                )
            else:
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
            if "issuers" in response.json()["result"]:
                df = pd.DataFrame(response.json()["result"]["issuers"])
            else:
                df = pd.DataFrame(
                    columns=[
                        "CLIENT_IDENTIFIER",
                        "ISSUERID",
                        "ISSUER_ISIN",
                        "GICS_SUB_IND",
                    ]
                )
            self.df = pd.concat([self.df, df], ignore_index=True)

    def load_historical(self, **kwargs) -> None:
        """
        Load historical data from MSCI API and save as pd.DataFrame in self.df
        """
        # generate token
        auth_token = self.request_authorization()

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer %s" % auth_token,
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
            logging.log(f"Batch: {batch+1}/{batches}")
            url = f"https://api.msci.com/esg/data/v2.0/issuers/history"
            js = {"batch_id": batch + 1, "data_request_id": data_request_id}
            r_batch = requests.request(
                "POST", url, headers=headers, json=js, verify="quantkit/certs.crt"
            )
            try:
                r = r_batch.json()["result"]["data"]
            except:
                continue
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
                data=data_list,
                columns=["CLIENT_IDENTIFIER", "Factor", "Value", "As_Of_Date"],
            )
            self.df = pd.concat([self.df, df], ignore_index=True)

        self.df = self.df.pivot(
            index=["CLIENT_IDENTIFIER", "As_Of_Date"], columns="Factor", values="Value"
        ).reset_index()
