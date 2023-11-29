# Asset Allocation

![AssetAllocation](../img/asset_allocation.png)  

## Settings
---
QuantKit users have the flexibility to employ and execute the package in two distinct ways. For additional details, refer to this [demo notebook](https://ml.azure.com/fileexplorerAzNB?wsid=/subscriptions/9e6414f9-fa32-459d-87f7-26856c9ebc31/resourceGroups/rg-sub-ae-shared-dev-001-esgmlws/providers/Microsoft.MachineLearningServices/workspaces/mlw-sub-ae-shared-dev-001-esgmlws&tid=b730b432-2098-413f-bd4a-014acdf7c72e&activeFilePath=Users/Tim.Bastian/quantkit/asset_allocation.ipynb).

### Configs-File Set-Up
Before executing the Asset Allocation functionality, ensure proper configuration of your local configs file. Specify the following parameters:

- API-Settings: Set Snowflake parameters

```shell

    "API_settings": {
        "snowflake_parameters": {
            "user": "user_name",
            "password": "password"
        }
    }

```

### Datasources

The asset allocation tool uses a variaty of datasources.

- Fundamental Data: Quandl
- Price Data: Quandl
- Economic Data: FRED
- Holdings Data: Internal Snowflake
- Security Data: MSCI
- ESG Data: MSCI

<details>
  <summary><b>For Nerds</b></summary>
The developer can switch in and out datasources in the configs file. The data is loaded through the iter function in the runner.

```python

    def iter(self) -> None:
        """
        iterate over DataFrames and create connected objects
        """
        self.iter_parent_issuers()
        self.iter_portfolios()
        self.iter_msci()
        self.iter_prices()
        self.iter_fundamentals()
        self.iter_holdings()
        self.iter_securities()
        self.iter_cash()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()
        self.init_strategies()

```

To change the datasource, make the following changes in your configs file as advised in the configs section of the general README.

```shell

    "prices_datasource": {
        "source": 3,
        "table_name": "PRICES",
        "database": "SANDBOX_ESG",
        "schema": "ESG",
        "load": true
    },
    "fundamentals_datasource": {
        "source": 3,
        "table_name": "FUNDAMENTAL_DATA",
        "database": "SANDBOX_ESG",
        "schema": "ESG",
        "load": true
    },

```

</details>

### Universe
The first step is to define an appropriate trading universe of securities. Users have various options to designate this universe by incorporating different settings in the configuration file. Additionally, users can set a start and end date to run the universe.

- Backtesting Timeframe: Define the months for which universe data should be retrieved.

```shell

    "universe_datasource": {
        "start_date": "01/01/2018",
        "end_date": "11/20/2023"
    }

```

- TCW Portfolios, optional: For TCW Portfolios, specify the portfolios on which the asset allocation framework should be executed on. Defaults to [""]. If the user wishes to run it on all available TCW portfolios, enter an empty list [], and if the user wants to exclude all available TCW portfolios, enter [""].

```shell

    "universe_datasource": {
        "tcw_universe": [""]
    }

```

- Equity Benchmark, optional: The user designates the equity benchmark indices on which the framework should be executed. Defaults to "S & P 500 INDEX". If the user wants to exclude all available benchmark portfolios, enter [].

```shell

    "universe_datasource": {
        "equity_universe": ["Russell 1000"]
    }  

```

- Fixed Income Benchmark, optional: The user designates the fixed income benchmark indices on which the framework should be executed. Defaults to []. If the user wants to exclude all available benchmark portfolios, enter [].

```shell

    "universe_datasource": {
        "fixed_income_universe": []
    }

```

- Custom Universe, optional: Users can define their own universe by inputting security ISINs. Defaults to [].

```shell

    "universe_datasource": {
        "custom_universe": ["US0378331005", "US5949181045", "US0231351067", 
                            "US30303M1027", "US46625H1005", "US0846707026",
                            "US02079K1079", "US02079K3059", "US4781601046",
                            "US30231G1022"]
    }

```

- Sustainable Universe, optional: Users have the choice to narrow down the universe to TCW's sustainable universe by incorporating blue and green tagged securities. Defaults to false.

```shell

    "universe_datasource": {
        "sustainable": false
    }

```

<details>
  <summary><b>For Nerds</b></summary>
The developer can find the code in quantkit > asset_allocation > universe. <br>
Upon initialization, a universe dataframe (r.portfolio_datasource.universe_df) is generated, indicating whether the security is held in the universe for a specified date (the last day of the month). In the case of the custom universe, we assume a consistent universe over time.

| As Of Date | AAPL | MSFT | XOM | META | JNJ | BRK.B | AMZN | JPM | GOOGL |
:------------|:-----|:-----|:----|:-----|:----|:------|:-----|:----|:------|
2018-01-31 00:00:00 | True | True | True | True | True | True | True | True | True |
2018-02-28 00:00:00 | True | True | True | True | True | True | True | True | True |
2018-03-31 00:00:00 | True | True | True | True | True | True | True | True | True |

When iterating over the trading dates, the `outgoing_row` function in the `universe` package yields the corresponding universe for that date. It achieves this by maintaining the current universe and updating it when the date reaches the next specified universe date.

```python

def outgoing_row(self, date: datetime.date) -> np.array:
    """
    Return current consitutents of index universe

    Parameters
    ----------
    date: datetimte.date
        date

    Returns
    -------
    np.array
        current constitutents of universe
    """
    if date >= self.universe_dates[self.current_loc + 1]:
        self.current_loc += 1
    return self.universe_matrix[self.current_loc]

```
</details>


### Strategies

Strategies are essential for every asset allocation tool. The quantkit asset allocation tool currently is limited to the following strategies. The parameters for the strategies can be set in the `strategies` section of the configs file.

<details>
  <summary><b>For Nerds</b></summary>

Every strategy needs the following functions:

- An `assign` function that assigns returns to return, risk and portfolio engines.
```python

    def assign(
        self,
        date: datetime.date,
        price_return: np.array,
        annualize_factor: int = 1.0,
    ) -> None:
        """
        Transform and assign returns to the actual calculator

        Parameters
        ----------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        annualize_factor: int, optional
            factor depending on data frequency
        """

```

A `selected_securites` function that returns an array of the index of all selected securities for that strategy and date. The developer should make sure that no securities with missing return data should be selected.

```python

    @property
    def selected_securities(self) -> np.array:
        """
        Index (position in universe_tickers as integer) of selected securities

        Returns
        -------
        np.array
            array of indexes
        """

```

A `return_metrics_optimizer` function that forecasts the returns for that particular strategy.

```python

    @property
    def return_metrics_optimizer(self) -> np.array:
        """
        Forecaseted returns for selected securities

        Returns
        -------
        np.array
            returns
        """

```


</details>

#### Momentum

The momentum strategy follows the motto "Buy Low, Sell High." The strategy picks the `top_n` securities in a rolling window of `window_size` based on cumulative returns.

```shell

    "strategies": {
        "momentum90": {
            "type": "momentum",
            "window_size": 63,
            "return_engine": "cumprod",
            "risk_engine": "log_normal",
            "top_n": 50,
            "allocation_models": [
                "equal_weight", 
                "market_weight", 
                "min_variance", 
                "constrained_min_variance", 
                "mean_variance", 
                "constrained_mean_variance", 
                "risk_parity"
            ]
        }
    }

```

<details>
  <summary><b>For Nerds</b></summary>

Momentum selects the `top_n` securities based on cumulative returns. We are sorting the return array by negative returns, which puts the highest return on top. We then pick the `top_n` securities, as long as they don't have missing data and they belong to the defined universe for that date. 

```python

    @property
    def selected_securities(self) -> np.array:
        """
        Index (position in universe_tickers as integer) of top n momentum securities

        Returns
        -------
        np.array
            array of indexes
        """
        nan_sum = np.isnan(self.latest_return).sum()
        top_n = min(self.top_n, self.num_total_assets - nan_sum)
        neg_sort = (-self.return_metrics_intuitive).argsort()

        selected_assets = 0
        i = 0
        a = list()

        while selected_assets < top_n:
            if self.index_comp[neg_sort[i]]:
                a.append(neg_sort[i])
                selected_assets += 1
            i += 1
        return np.array(a)

```


</details>

#### Pick All

Pick all available securities in universe.

```shell

    "strategies": {
        "pick_all": {
            "type": "pick_all",
            "window_size": 63,
            "return_engine": "log_normal",
            "risk_engine": "log_normal",
            "allocation_models": ["equal_weight", "market_weight"]
        }
    }

```

<details>
  <summary><b>For Nerds</b></summary>

The strategy picks all securities available for that month without missing data.

```python

    @property
    def selected_securities(self) -> np.array:
        """
        Index (position in universe_tickers as integer) of selected securities

        Returns
        -------
        np.array
            array of indexes
        """
        ss = np.arange(self.num_total_assets)
        return ss[~np.isnan(self.latest_return) & self.index_comp]

```


</details>

### Return and Risk Calculation



### Optimizers


- Weight Constraints, optional: The allowable range of weights that an asset can take on in constrained weighting strategies at any point in time.

```shell

    "default_weights_constraint": [0.001, 0.15]

```

### Backtesting

After assigning the returns to the optimizers, the asset allocation package calculates a portfolio return based on the selected assets. This allows a regirous backtesting capability.  First, the user can set the following parameters: 

- Rebalance Period: Can either be set to "DAY", "MONTH", "QUARTER",  "YEAR"

```shell

    "prices_datasource": {
        "rebalance": "MONTH"
    }

```
- Transaction Costs, optional: Transaction costs per trade refer to the expenses associated with each individual trade.

```shell

    "trans_cost": 0.01

```

The allocation tool then calculates daily portfolio returns based on current allocation and past allocation.

```python

            ex_ante_portfolio_return = self.get_portfolio_stats(
                ex_ante_allocation, ex_post_allocation
            )

```

<details>
  <summary><b>For Nerds</b></summary>

The returns are calculated in the following way. `this_returns` is an array of daily returns for all securities in the universe.

```python

    def get_portfolio_return(
        self,
        allocation: np.array,
        this_returns: np.array,
        indexes: np.array,
        next_allocation: np.array = None,
        trans_cost: float = 0.0,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Calculate 0 basis portfolio return
        Return a DataFrame with returns in frequency for each date in rebalance window

        Parameters
        ----------
        allocation: np.array
            current allocation
        this_returns: np.array
            forecasted returns per asset
        indexes: np.array
            index column for returned DataFrame
            should be set to date range
        next_allocation: np.array, optional
            next allocation, used to calculate turnover and transaction costs
        trans_cost: float, optional
            transaction cost in %

        Returns
        -------
        pd.DataFrame
            return: float
        """
        n_obs = len(this_returns)
        cumulative_returns = np.cumprod(this_returns + 1, axis=0)
        cumulative_returns = np.where(
            np.isnan(cumulative_returns), 0, cumulative_returns
        )
        ending_allocation = allocation * cumulative_returns
        # Normalize ending allocation
        ending_allocation = (
            ending_allocation.T / np.nansum(ending_allocation, axis=1)
        ).T

        actual_returns = allocation @ cumulative_returns.T
        actual_returns = np.insert(actual_returns, 0, 1)
        actual_returns = np.diff(actual_returns) / actual_returns[:-1]

        # Subtract transaction costs
        next_allocation_m = copy.deepcopy(ending_allocation)
        next_allocation_m[-1] = next_allocation
        trans_cost_m = np.zeros((n_obs, self.universe_size))
        trans_cost_m[-1] = trans_cost
        if next_allocation is not None:
            turnover = abs(next_allocation_m - ending_allocation)
            this_trans_cost = (turnover * trans_cost_m).sum(axis=1)
            actual_returns -= this_trans_cost

        return pd.DataFrame(data=actual_returns, index=indexes, columns=["return"])


```


</details>

## Usage

### The Object Method
Proficient users can leverage the structure of objects generated within the code. Begin by initializing a runner object using the following method.

```python

import quantkit.runners.runner_PAI as runner

local_configs = "path\\to\\your\\configs.json"

r = runner.Runner()
r.init(local_configs=local_configs)
r.run()

```

This establishes connections to all databases and executes the calculations. To retrieve datapoints, we can inspect the objects. We access a portfolio object in the following way.

```python

r.portfolio_datasource.portfolios["3750"].impact_data

```

### The handyman Folder

The `handyman` folder serves as a user-friendly option for those solely interested in the output rather than the objects. It offers functions to execute the PAI framework and obtain pertinent data without navigating through the objects. To run the PAI framework and obtain a detailed DataFrame, execute the `principal_adverse_impact()` function available in the `pai` package.

```python

import quantkit.handyman.pai as pai

local_configs = "path\\to\\your\\configs.json"

df = pai.principal_adverse_impact(local_configs)

```

To obtain the original data points for each security of each portfolio utilized in the calculation, execute the following function.

```python

import quantkit.handyman.pai as pai

local_configs = "path\\to\\your\\configs.json"

df = pai.principal_adverse_data_points(local_configs)

```