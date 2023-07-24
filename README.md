# quantkit

## Table of Contents
<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#description">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
        <li><a href="#relevant-links">Relevant Links</a></li>
      </ul>
    </li>
    <li><a href="#support">Support</a></li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
        <li><a href="#configarations">Configarations</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#authors-and-acknowledgment">Authors and Acknowledgement</a></li>
  </ol>
</details>

## About the Project
---
The quantkit project aims to combine data operations such as data pulling through API usage, data transformations and validations with high-level calculations such as risk measurement. The SIG's team risk framework calculation is build on top of quantkit. Quantkit aims to make quantitative research easier by giving easy-to-use functions to retrieve and play around with data from various data sources, such as MSCI, Quandl and Snowflake databases.

### Built With
[![python][python]][python-url]

### Relevant Links
[![Git][git]][git-url]
[![Azure][ml-azure]][ml-azure-url]
[![Power Bi][power-bi]][power-bi-url]
[![Snowflake][snowflake]][snowflake-url]

<p align="right">(<a href="#quantkit">back to top</a>)</p>

## Support
---
- For further questions, please send an email to [tim.bastian@tcw.com](mailto:tim.bastian@tcw.com?cc=Sherveen.Abdarbashi@tcw.com&subject=quantkit)
- For feature recommendations or bug fixes, please open an [issue](https://gitlab.com/tcw-group/quant-research/quantkit/-/issues).
- For recent changes, please check the [CHANGELOG](CHANGELOG.md). <br>

<p align="right">(<a href="#quantkit">back to top</a>)</p>

## Getting Started 
---

### Local Environment
---

#### Prerequisites
- IDE: the following steps are performed in Visual Studio Code (VSC). The use of every other IDE works as well, but different steps may be necessary. 
- Install Anaconda: It is recommended, but not needed, to install anaconda from [here](https://conda.io/projects/conda/en/latest/user-guide/install/index.html). Make sure to select the version for your operating system. In the window below, add anaconda to your PATH environmental variable and finish the setup.
![Anaconda](img/anaconda.png)  
- Create Environment: create a project specific environment through anaconda. Open a command line and type the following command. You can close the command line after.
```shell
> conda create -n "quantkit" ipython -y
```
- git: a working version of git installed on your computer is required. Please request access to [the quantkit folder](https://gitlab.com/tcw-group/quant-research/quantkit) in gitlab, if not already provided.

### Installation
- Open VSC (or your IDE of choice) 
- Activate Environment: In VSC, open a new window. Open the folder you want quantkit to be installed in. On the top, go to Terminal > New Terminal. In the terminal, type the following command:
```shell
> conda activate quantkit
```
- Select python Interpreter: Press CTRL+SHIFT+P on your keyboard. A window will open from the top. Type Python: Select Interpreter and click it. Choose your conda quantkit environment.
- Head to the gitlab folder of [quantkit](https://gitlab.com/tcw-group/quant-research/quantkit) and clone the repo: Clone > Clone with HTTPS > Copy URL
- In a terminal in VSC, type the following command:  
```shell
> git clone copied_path_from_step_above
```
- This clones the repository to your machine. Activate the development branch by typing the following commands into the terminal:
```shell
> cd quantkit
> git checkout develop
```
- install the requirements: in the terminal, type the following command. Make sure the quantkit environment is still activated.
```shell
> pip install -r requirements.txt
```
- ATTENTION: please do not make changes to the development branch and push them. If you want to contribute and make changes to the code, please follow the guidelines in [CONTRIBUTING](CONTRIBUTING.md).

### Configarations
The configarations file includes all the settings a user can change. It is located in quantkit > utils > configs.json.

#### Local configs file
It is recommended to use a local configs.json file and not overwriting the parameters in the original file. To do so, open Notepad on your machine, type in `{}`, and save it as configs.json on your local system. Next, go to the original configs file and change the configs_path parameters to link to your newly created local configs file.

```json
{
      "configs_path":"path_to_your_file.json"
}
```

#### The configs file
The user is able to change keys for API usage, thresholds for calculations, datasources, etc. First, make sure the settings are right. If you need to make changes to the configs file, please do so to the local file you created in the step above. For example, to change the portfolio_datasource, copy over the portfolio_datasource part you want to change into your local file.
The source numeration works as follows:
1. Excel
2. CSV
3. Snowflake
4. MSCI API
5. Quandl API
6. JSON

So, if you want to change the portfolio_datasource to snowflake, enter the following paramaters to your local file:

```json
    "portfolio_datasource": {
        "source": 3,
        "table_name": "TABLE_NAME" ,
        "load": true 
    },

```

add to portfolio file
<p align="right">(<a href="#quantkit">back to top</a>)</p>

### ML Azure Environment
---
### Prerequisites
- ML Azure access: make sure you have access to ML Azure and can run code in there.
### Installation
- Head to the gitlab folder of [quantkit](https://gitlab.com/tcw-group/quant-research/quantkit) and clone the repo: Clone > Clone with HTTPS > Copy URL
- in ML Azure, open the folder you want the quantkit code to be in, right click the folder and click Open Terminal
- In the terminal type the following command:
```shell
> git clone copied_path_from_step_above
```
- This clones the repository to your machine. Activate the ml_azure branch by typing the following commands into the terminal:
```shell
> cd quantkit
> git checkout ml_azure
```
- You might be asked to add an excemption for this directory in order to run the command above. Copy and run the command provided in the error message and run the checkout again after.
- Create a notebook inside your top folder on the same level as quantkit.
- Select Python 3.8 - ML Azure as your default kernel in the right top corner. 
- install the requirements: In a cell, type in the following command:
```shell
pip install -r "quantkit/requirements.txt"
```
- ATTENTION: please do not make changes to the ml_azure branch and push them. If you want to contribute and make changes to the code, please follow the guidelines in [CONTRIBUTING](CONTRIBUTING.md).

### Configarations

<p align="right">(<a href="#quantkit">back to top</a>)</p>

## Update Version
---
As the framework develops, there will be changes to the code base too. To get the newest version of the code, you need to pull from the gitlab repository.

### Local Environment
- In VSC, open a Terminal from your working folder und run the following commands:
```shell
> cd quantkit
> git pull
```

### ML Azure Environment
- In ML Azure, open a Terminal from your working folder und run the following commands:
```shell
> cd quantkit
> git pull
```
- You are prompted to enter User Name and Password to your git account linked to gitlab.

## Usage
---
Quantkit users can use and run the package in two ways. For more details, please see this [demo notebook](https://ml.azure.com/fileexplorerAzNB?wsid=/subscriptions/9e6414f9-fa32-459d-87f7-26856c9ebc31/resourceGroups/rg-sub-ae-shared-dev-001-esgmlws/providers/Microsoft.MachineLearningServices/workspaces/mlw-sub-ae-shared-dev-001-esgmlws&tid=b730b432-2098-413f-bd4a-014acdf7c72e&activeFilePath=Users/Tim.Bastian/quantkit/demo.ipynb).

### The Object Method
The experienced user can make use of the objects structure itself created in the code. First, initialize a runner object in the following way.

```python
import quantkit.runner as runner

r = runner.Runner()
r.init() 
r.run()
```
This connects to all the databases and runs the calculations. To access the datapoints, we can "look" into the objects. First, we access a portfolio object.

```python
r.portfolio_datasource.portfolios[16705].holdings
```
This query returns all the holdings of portfolio 16705 in a dictionary.

```shell
{'Cash': {
    'object': <quantkit.finance.securities.securities.SecurityStore at 0x2257288cf10>,
    'holding_measures': [{
      'Portfolio_Weight': 1.741434,
      'Base Mkt Val': 215827.45,
      'OAS': 0.0
      }]
    },
 'US72352L1061': {
    'object': <quantkit.finance.securities.securities.EquityStore at 0x225737bc0a0>,
    'holding_measures': [{
      'Portfolio_Weight': 2.449617,
      'Base Mkt Val': 303597.25,
      'OAS': 0.0
    }]
  },
 ...
}
```
The portfolio holdings above showcase another object type, the security object. To see information about a security, run the following line.

```python
r.security_datasource.securities["US88160R1014"].information
```

This returns a dictionary of information about the security including p.e. issuer name, seucrity isin, ticker and SClass Levels.
```shell
{'ISSUER_CUSIP': '88160R101',
 'ISSUER_SEDOL': 'B616C79',
 'ISSUER_ISIN': 'US88160R1014',
 'ISSUER_CNTRY_DOMICILE': 'US',
 'ISSUERID': 'IID000000002594878',
 'IssuerName': 'TESLA, INC.',
 'ISIN': 'US88160R1014',
 'Ticker': 'TSLA',
 'issID': 601399.0,
 'IssuerLEI': '54930043XZGB27CTOV49',
 'Security ISIN': 'US88160R1014',
 'Security Type': 'Equity',
 'ESG_Collateral_Type': {'G/S/S': 'Unknown',
  'ESG Collat Type': 'Unknown',
  'Sustainability Theme - Primary': 'Unknown',
  'Sustainability Theme - Secondary ': 'Unknown',
  'Sclass_Level3': 'Unknown',
  'Primary': 'Unknown',
  'Secondary': 'Unknown'},
 'Labeled_ESG_Type': nan,
 'TCW_ESG': nan,
 'Issuer_ESG': 'No',
 'Security_Name': 'TESLA INC',
 'SClass_Level1': 'Preferred',
 'SClass_Level2': 'Sustainable Theme',
 'SClass_Level3': 'Multi-Thematic',
 'SClass_Level4': 'Planet',
 'SClass_Level4-P': 'MOBILITY',
 'SClass_Level5': 'Unknown'}
```
Other callable objects include
- Company objects
```python
r.portfolio_datasource.companies["US88160R1014"]
```
- Industry objects
```python
r.gics_datasource.industries["Biotechnology"]
r.bclass_datasource.industries["Consumer Cyclical"]
```
- Theme objects
```python
r.theme_datasource.themes["BIODIVERSITY"]
```

- Region objects
```python
r.region_datasource.regions["DE"]
```

### The handyman Folder
The handyman folder is an easy-to-use alternative if the user is just interested in the output and not the objects. It provides functions to run the framework and get relevant data back without going through the objects. To run the risk framework and return a detailed DataFrame, simply run the risk_framework() function provided in the risk_framework package.

```python
import quantkit.handyman.risk_framework as risk_framework

# run risk framework
df_detailed = risk_framework.risk_framework()
```
To only run the framework on specific isins, the user can make use of the isin_lookup() function in the risk_framework package. This returns a DataFrame with relevant information about the inputted securities.

```python
# isin lookup
isins = ["US88160R1014", "US0378331005"]
df_isin = risk_framework.isin_lookup(isins)
```

<p align="right">(<a href="#quantkit">back to top</a>)</p>

## Roadmap
---
- [ ] Write full comprahensive README
- [ ] Add to CONTRIBUTING file
- [ ] Push first version of quantkit
- [ ] Add to mathstats folder for mathematical calculations
- [ ] create data visualization folder
- [ ] add functions to handyman folder
- [ ] update data pipelines (connect portfolio data to snowflake)

See the [open issues](https://gitlab.com/tcw-group/quant-research/quantkit/-/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#quantkit">back to top</a>)</p>

## Contributing
---
For contributing guidelines, please check [CONTRIBUTING](CONTRIBUTING.md).

<p align="right">(<a href="#quantkit">back to top</a>)</p>

## Authors and acknowledgment
---
The risk framework is a collective effort of the Sustainable Investment Group (SIG). For further questions, please contact [ESGAnalysts@tcw.com](mailto:ESGAnalysts@tcw.com).

<p align="right">(<a href="#quantkit">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[python]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54
[python-url]: https://www.python.org/
[git]: https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white
[git-url]: https://gitlab.com/tcw-group/quant-research/quantkit/-/tree/develop?ref_type=heads
[ml-azure]: https://img.shields.io/badge/ml%20azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white
[ml-azure-url]: https://ml.azure.com/fileexplorerAzNB?wsid=/subscriptions/9e6414f9-fa32-459d-87f7-26856c9ebc31/resourceGroups/rg-sub-ae-shared-dev-001-esgmlws/providers/Microsoft.MachineLearningServices/workspaces/mlw-sub-ae-shared-dev-001-esgmlws&tid=b730b432-2098-413f-bd4a-014acdf7c72e&activeFilePath=Users/Tim.Bastian/quantkit/test.ipynb
[power-bi]:https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black
[power-bi-url]: https://app.powerbi.com/groups/b47086b3-af81-40ba-901c-19e60543ea94/list?experience=power-bi
[snowflake]: https://img.shields.io/badge/Snowflake-22ADF6?style=for-the-badge&logo=InfluxDB&logoColor=white
[snowflake-url]: https://app.snowflake.com/tcw/titan/#/data/databases/ESG_SANDBOX/schemas/ESG