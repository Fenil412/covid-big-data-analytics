# Dataset

## Source

Google COVID-19 Open Data

https://github.com/GoogleCloudPlatform/covid-19-open-data

## Selected Data

The project will primarily use the Google COVID-19 Open Data
epidemiology, vaccination and hospitalization tables.

## Data Handling

Raw datasets are intentionally not stored in GitHub because of
their size.

The raw data will be downloaded separately and uploaded to Hadoop HDFS
during the ingestion phase.

## HDFS Target

/covid/raw/