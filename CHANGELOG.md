# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Calculate Return Statistics
- Quandl Handyman functions
### Fixed
- UNION to UNION ALL
### Changed
- run scaling in HRP Optimizer
- Rename parent ticker mapping columns
- Quant Research Snowflake Scheme
- use datekey as release date
### Removed

## [1.5.2] - 2023-12-26
### Added
- Original Weight Weighting scheme
- Add Is Valid validation in risk and return engines
- Add Area Plot for weights
- Allow user to set weight constraints on asset level
- Scaled HRP
- Leverage Functionality
### Fixed
- Fixed Monthly vs. Daily Bug in Covariance Matrix
### Changed
- Only update return and risk engine once, not in every strategy
- Move Stop-Loss functionality in seperate folder
- Changed way of iterating portfolios and holdings
### Removed

## [1.5.1] - 2023-12-18
### Added
- Simple and Rolling Cumulative Sum
- Stop-Loss Logic: High-to-Low, Buy-to-Low
- Add Factor Datasource
- Calculate Factor betas for each allocation in strategies
- Add MSCI Issuerid to final df 
### Fixed
### Changed
- Rename testing file to test_mathstats
- Remove Sub-Folder for finance datsources
### Removed

## [1.5.0] - 2023-12-05
### Added
- Documentation for Risk Framework, PAI and Asset Allocation
- Relative Value Strategy
- Market Multiple Datasource
- Correlation test in test file
- Hierarchical Risk Parity allocator
- TCW Equity Portfolio Adjustments
### Fixed
- MSCI Datasource empty return
- Numpy array value specification to np.ndarray
- Momentum top_n bug
### Changed
- README
- Snowflake pull function uses either query or database/schema/tablename
- MSCI Dataloader allow specified factors
- Specific Configs files for risk_framework, asset_allocation, pai
- Move main runner into runners folder
- empty list portfolios = load nothing, ["all] = load all
### Removed

## [1.4.0] - 2023-11-24
### Added
- JPM CEMBI BROAD DIVERSE and PM EMBI GLOBAL DIVERSIFI added to portfolio df
- Universe Datasource in asset allocation tool
- Allow custom universe
- Make universe change over time 
- Allocation history per strategy attached to each security
### Fixed
### Changed
- Split QuandlDataSource into PricesDataSource and FundamentalsDataSource
- Use numpy array for date iterations
### Removed

## [1.3.3] 2023-11-16
### Added
- Snowflake util function to update daily DataFrame
- Run Framework on all portfolios
- Add Local Authority to carveout list
### Fixed
### Changed
- Snowflake datasource and util functions to use snowflake.connector
- add up weight and base mkt value for same security in portfolio instead of duplicating security
- add start date and end date to portfolio datasource load

### Removed

## [1.3.2] - 2023-10-30
### Added
### Fixed
### Changed
- Change Snowflake schema from _DEV to _QA
- Change Theme long labels
- Change Schema for detailed df
- Use Securitized Mapping Table for collat types instead of hard coded lists
- set default date to last business day, make overwriteable in configs file
### Removed

## [1.3.1] - 2023-10-10
### Added
- handyman function to save PAI raw datapoints
### Fixed
- bug in snowflake utils
### Changed
- SDG datasource to use Snowflake data instead of Excel file
### Removed

## [1.3.0] - 2023-10-06
### Added
### Fixed
### Changed
- Exclusion Datasource use MSCI API instead of Excel file
- Don't overwrite Excluded Sector when creating detailed DataFrame
- Overwrite Excluded Sector when pdf-dashboarding
### Removed
- carve out sectors from configs file
- A8 and A9 funds from configs file

## [1.2.1] - 2023-10-05
### Added
- handyman function to pull historical portfolio holdings
- Unadjusted scores added to detailed dataframe
### Fixed
- make pathes working in ML Azure
- BClass replace N/A's
- add Labeled Sustainable/Sustainable Linked to labeled bonds
### Changed
- Split Excluded SClass Labels into Poor Data, Poor Score, and Excluded Sector
### Removed


## [1.2.0] - 2023-09-29
### Added
- PDF page for Equity MSCI
### Fixed
### Changed
- Portfolio Data comes from Snowflake table
- Use Portfolio Datasource to create security and company objects
### Removed
- security datasource

## [1.1.1] - 2023-09-21
### Added
- Pick-All strategy
- OLS linear regression calculation 
- ridge regression calculation 
- test folder for mathstats
- overwrite history function for Snowflake
- handyman function for unadjusted scores

### Fixed

### Changed
- allow quandl pull without ticker list
- add MSCI CCC screen for Sovereigns

### Removed



## [1.1.0] - 2023-09-12
### Added
- asset allocation folder including:
    - weighting scheme calculators (equal weight, market weight, MVO, min variance, risk parity)
    - return and risk calculator (cumprod, EWMA, simple, log)
    - strategies (simple momentum)
- mathstats folder including:
    - mean and covariance calcutor (simple, rolling, exponential)
    - optimizer
    - portfolio stats
    - cumprod
    - streaming base
    - time series calculation
- visualization function for asset allocation
- asset allocation runner

### Fixed
- bugs in pdf creator
- bugs in PAI
- fill na's by row of same security

### Changed
- all runners into seperate runner folder
- adding analyst adjustment for sovereigns
- enable flexible queries to load data from Snowflake


### Removed

## [1.0.1] - 2023-08-23

### Added
- load historical data through MSCI API
- run snowflake locally with python 3.10 environment
- Quandl handyman functionality
- FRED API and handyman functionality
- PDF creator visualization tool
- PAI calculation

### Fixed

### Changed
- Static Q-Low, Q-High and median values on BCLASS and GICS level in transition calculation
- move iteration into loader

### Removed
- ml_azure branch
- median calculation for industries

## [1.0.0] - 2023-08-09

### Added
- APIs to read data from various sources, such as excel, csv, json, MSCI, quandl, and snowflake
- handyman functions for easy use of functionality
- mathstats folder for mathematical calculations such as mean, median, covariance
- logic to iter over data and calculate risk framework output
- README, CONTRIBUTING, CHANGELOG files
- First push of quantkit

### Fixed

### Changed

### Removed
- original risk framework calculation 


## [0.0.1] - 2023-05-01

### Added
- original risk framework code
- pandas based approach to calculate risk metrics

### Fixed

### Changed

### Removed


[unreleased]: https://gitlab.com/tcw-group/quant-research/quantkit/-/compare/main...develop?from_project_id=46798372&straight=false
[1.5.2]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.5.2
[1.5.1]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.5.1
[1.5.0]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.5.0
[1.4.0]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.4.0
[1.3.3]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.3.3
[1.3.2]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.3.2
[1.3.1]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.3.1
[1.3.0]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.3.0
[1.2.1]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.2.1
[1.2.0]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.2.0
[1.1.1]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.1.1
[1.1.0]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.1.0
[1.0.1]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.0.1
[1.0.0]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.0.0
[0.0.1]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/0.0.1
