import requests
from base64 import b64encode
import json

b64login = b64encode(
    bytes(
        "%s:%s"
        % (
            "b769e9c4-16c4-45f8-89a5-6f555587a640",
            "0a7251dfd9dadd7f92444cab7e665e875a38ff5a",
        ),
        encoding="utf-8",
    )
).decode("utf-8")

# url = "https://api.msci.com/esg/data/v1.0/funds?category_path_list=ESG+Ratings:Company+Summary&format=json"
# headers = {"Accept": "application/json", "Authorization": "Basic %s" % b64login}

# response = requests.request("POST", url, headers=headers, verify="certs.crt")
# print(response.text)


url = "https://api.msci.com/esg/data/v1.0/parameterValues/countries"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Basic %s" % b64login,
}

payload = json.dumps({})
response = requests.request(
    "POST", url, headers=headers, data=payload, verify="certs.crt"
)
print(response.text)
