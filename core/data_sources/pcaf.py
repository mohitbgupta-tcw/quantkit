import os
import requests
import pandas as pd
from pathlib import Path
from requests.adapters import Retry, HTTPAdapter
import zipfile
import re
import quantkit.utils.logging as logging
from os import listdir


class PCAF(object):
    """
    Main class to load PCAF data using the PCAF API
    When running locally, make sure to add certificate certs.crt

    Parameters
    ----------
    key: str
        pcaf api key
    url: str
        pcaf url to get data from
    version: str, optional
        version to pull

    """

    def __init__(self, key: str, url: str, version: str = "latest", **kwargs) -> None:
        self.key = key
        self.url = url
        self.version = version
        self.headers = {"API-Key": self.key}
        self.initialize_session()

    def initialize_session(self) -> None:
        """
        Initialize requests Session
        """
        d = Path(__file__).resolve().parent.parent.parent

        # Initialize a requests
        self.session = requests.Session()
        retry = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)

        params = {"getmetadata": "1"}

        if os.name == "posix":
            response = self.session.get(self.url, headers=self.headers, params=params)
        elif os.name == "nt":
            response = self.session.get(
                self.url, headers=self.headers, params=params, verify=f"{d}\\certs.crt"
            )
        else:
            response = self.session.get(
                self.url, headers=self.headers, params=params, verify=f"{d}\\certs.crt"
            )

        # if metadata request was successful, proceed to process metadata
        if response.status_code == 200:
            self.metadata = response.json()

        else:
            try:
                message = f"Download failed. Status code: {response.status_code} and reason: {response.json()[0]['Error']}"
                logging.log(message)
            except:
                message = f"Download failed. Status code: {response.status_code} and repsonse:{response.content}"

    def load(self, asset_class: str, **kwargs) -> None:
        """
        Load data from PCAF API and save as pd.DataFrame in self.df

        Parameters
        ----------
        asset_class: str
            asset class number
        """
        d = Path(__file__).resolve().parent.parent.parent

        version = (
            len(self.metadata["version"]) - 1
            if self.version == "latest"
            else self.metadata["version"].index(self.version)
        )
        params = {
            "getdata": "1",
            "asset_class": asset_class,
            "version": version,
            "format": "csv",
        }

        if os.name == "posix":
            response = self.session.get(self.url, headers=self.headers, params=params)
        elif os.name == "nt":
            response = self.session.get(
                self.url, headers=self.headers, params=params, verify=f"{d}\\certs.crt"
            )
        else:
            response = self.session.get(
                self.url, headers=self.headers, params=params, verify=f"{d}\\certs.crt"
            )

        if response.status_code == 200:
            content_disposition = response.headers.get("Content-Disposition")
            if content_disposition:
                filename = re.findall("filename=(.+)", content_disposition)[0].strip(
                    '"'
                )

                with open(filename, "wb") as f:
                    f.write(response.content)

                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall("./downloads")
                os.remove(filename)

                file = listdir("./downloads")[0]
                self.df = pd.read_csv(f"./downloads/{file}")
                os.remove(f"./downloads/{file}")
                os.rmdir(f"./downloads/")
        else:
            try:
                message = f"Download failed. Status code: {response.status_code} and reason: {response.json()[0]['Error']}"
                logging.log(message)
            except:
                message = f"Download failed. Status code: {response.status_code} and repsonse:{response.content}"
