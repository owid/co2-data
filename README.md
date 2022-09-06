# Data on CO2 and Greenhouse Gas Emissions by *Our World in Data*

Our complete CO2 and Greenhouse Gas Emissions dataset is a collection of key metrics maintained by [*Our World in Data*](https://ourworldindata.org/co2-and-other-greenhouse-gas-emissions). It is updated regularly and includes data on CO2 emissions (annual, per capita, cumulative and consumption-based), other greenhouse gases, energy mix, and other relevant metrics.

## The complete *Our World in Data* CO2 and Greenhouse Gas Emissions dataset

### 🗂️ Download our complete CO2 and Greenhouse Gas Emissions dataset : [CSV](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv) | [XLSX](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.xlsx) | [JSON](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.json)

The CSV and XLSX files follow a format of 1 row per location and year. The JSON version is split by country, with an array of yearly records.

The variables represent all of our main data related to CO2 emissions, other greenhouse gas emissions, energy mix, as well as other variables of potential interest.

We will continue to publish updated data on CO2 and Greenhouse Gas Emissions as it becomes available. Most metrics are published on an annual basis.

A [full codebook](https://github.com/owid/co2-data/blob/master/owid-co2-codebook.csv) is made available, with a description and source for each variable in the dataset.

## Our source data and code

The dataset is built upon a number of datasets and processing steps:
- Statistical review of world energy (BP):
  - [Source data](https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html)
  - [Ingestion and processing code](https://github.com/owid/importers/tree/master/bp_statreview)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/bp/2022-07-14/statistical_review.py)
- International energy data (EIA):
  - [Source data](https://www.eia.gov/opendata/bulkfiles.php)
  - [Ingestion code](https://github.com/owid/walden/blob/master/ingests/eia_international_energy_data.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/eia/2022-07-27/energy_consumption.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/eia/2022-07-27/energy_consumption.py)
- Primary energy consumption (Our World in Data based on BP's Statistical review of world energy & EIA's International energy data):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/2022-07-29/primary_energy_consumption.py)
- Global carbon budget (Our World in Data based on Global Carbon Project):
  - [Source data](https://www.globalcarbonproject.org/carbonbudget/21/data.htm)
  - [Ingestion and processing code](https://github.com/owid/importers/tree/master/gcp_gcb)
- Greenhouse gas emissions (including methane and nitrous oxide) by sector (CAIT):
  - [Source data](https://www.climatewatchdata.org/data-explorer/historical-emissions)
  - [Ingestion code](https://github.com/owid/walden/blob/master/ingests/cait/2022-08-10/cait_ghg_emissions.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/cait/2022-08-10/ghg_emissions_by_sector.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/cait/2022-08-10/ghg_emissions_by_sector.py)
- CO2 dataset (Our World in Data based on all sources above):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/2022-08-05/owid_energy.py)
  - [Exporting code](https://github.com/owid/energy-data/blob/master/scripts/make_dataset.py)
  - [Uploading code](https://github.com/owid/energy-data/blob/master/scripts/upload_datasets_to_s3.py)

Additionally, to construct variables per capita and per GDP, we use the following datasets and processing steps:
- Population (Our World in Data based on [a number of different sources](https://ourworldindata.org/population-sources)).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/owid/latest/key_indicators/table_population.py)
- GDP (University of Groningen GGDC's Maddison Project Database, Bolt and van Zanden, 2020).
  - [Source data](https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020)
  - [Ingestion code](https://github.com/owid/walden/blob/master/ingests/ggdc_maddison.py)
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/ggdc/2020-10-01/ggdc_maddison.py)

## Changelog

- September 6, 2022:
  - Updated data on primary energy consumption (from BP & EIA) and greenhouse gas emissions by sector (from CAIT).
  - Refactored code, since now this repository simply loads the data, generates the output files, and uploads them to the cloud; the code to generate the dataset is now in our [etl repository](https://github.com/owid/etl).
  - Minor changes in the codebook.
- April 15, 2022:
  - Updated primary energy consumption data.
  - Updated CO2 data to include aggregations for the different country income levels.
- February 24, 2022:
  - Updated greenhouse gas emissions data from CAIT Climate Data Explorer.
  - Included two new columns in dataset: total greenhouse gases excluding land-use change and forestry, and the same as per capita values.
- November 5, 2021: Updated CO2 emissions data with the newly released Global Carbon Budget (v2021).
- September 16, 2021:
  - Fixed data quality issues in CO2 emissions variables (emissions less than 0, missing data for Eswatini, ...).
  - Replaced all input CSVs with data retrieved directly from ourworldindata.org.
- February 8, 2021: we updated this dataset with the latest annual release from the Global Carbon Project.
- August 7, 2020: the first version of this dataset was made available.

## Data alterations

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names in order to minimize data loss during data merges.
- **We recalculate carbon emissions to CO2.** The primary data sources on CO2 emissions—the Global Carbon Project, for example—typically report emissions in tonnes of carbon. We have recalculated these figures as tonnes of CO2 using a conversion factor of 3.664.
- **We calculate per capita figures.** All of our per capita figures are calculated from our metric `Population`, which is included in the complete dataset. These population figures are sourced from [Gapminder](http://gapminder.org) and the [UN World Population Prospects (UNWPP)](https://population.un.org/wpp/).

## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.

## Authors

This data has been collected, aggregated, and documented by Hannah Ritchie, Max Roser, Edouard Mathieu, Bobbie Macdonald and Pablo Rosado.

The mission of *Our World in Data* is to make data and research on the world’s largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).
