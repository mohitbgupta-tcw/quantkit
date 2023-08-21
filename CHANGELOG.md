# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- load historical data through MSCI API
- run snowflake locally with python 3.10 environment
- Quandl handyman functionality
- FRED API and handyman functionality

### Fixed

### Changed
- Static Q-Low, Q-High and median values on BCLASS and GICS level in transition calculation

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
[1.0.0]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/1.0.0
[0.0.1]: https://gitlab.com/tcw-group/quant-research/quantkit/-/releases/0.0.1
