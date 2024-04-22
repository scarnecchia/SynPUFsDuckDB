# SAS2Parquet

This is an informal scripted designed to convert the [CMS 2008-2010 Data Entrepreneurs' Synthetic Public Use Files (SynPUFs)](https://www.sentinelinitiative.org/methods-data-tools/software-packages-toolkits/medicare-claims-synthetic-public-use-files-sentinel-0) in the Sentinel Common Data Model (SCDM) format from SAS to Parquet without requiring SAS software to export the data to an intermediate format.

Please note that prior versions of this script simply appended a table to create the datasets. This does not work because ID variables were not mutually exclusive between the datasets. This [gist](https://gist.github.com/scarnecchia/f544350c5d65f934f75e0123ab836e8a) contains a more thorough write-up of the issue.
