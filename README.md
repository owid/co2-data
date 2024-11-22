# Data on CO2 and Greenhouse Gas Emissions by *Our World in Data*

Our complete CO2 and Greenhouse Gas Emissions dataset is a collection of key metrics maintained by [*Our World in Data*](https://ourworldindata.org/co2-and-other-greenhouse-gas-emissions). It is updated regularly and includes data on CO2 emissions (annual, per capita, cumulative and consumption-based), other greenhouse gases, energy mix, and other relevant metrics.

## The complete *Our World in Data* CO2 and Greenhouse Gas Emissions dataset

### 🗂️ Download our complete CO2 and Greenhouse Gas Emissions dataset : [CSV](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv) | [XLSX](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.xlsx) | [JSON](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.json)

The CSV and XLSX files follow a format of 1 row per location and year. The JSON version is split by country, with an array of yearly records.

The indicators represent all of our main data related to CO2 emissions, other greenhouse gas emissions, energy mix, as well as other indicators of potential interest.

We will continue to publish updated data on CO2 and Greenhouse Gas Emissions as it becomes available. Most metrics are published on an annual basis.

A [full codebook](https://github.com/owid/co2-data/blob/master/owid-co2-codebook.csv) is made available, with a description and source for each indicator in the dataset. This codebook is also included as an additional sheet in the XLSX file.

## Our source data and code

The dataset is built upon a number of datasets and processing steps:

- Statistical review of world energy (Energy Institute, EI):
  - [Source data](https://www.energyinst.org/statistical-review)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/energy_institute/2024-06-20/statistical_review_of_world_energy.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/energy_institute/2024-06-20/statistical_review_of_world_energy.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy_institute/2024-06-20/statistical_review_of_world_energy.py)
- International energy data (U.S. Energy Information Administration, EIA):
  - [Source data](https://www.eia.gov/opendata/bulkfiles.php)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/eia/2023-12-12/international_energy_data.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/eia/2023-12-12/energy_consumption.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/eia/2023-12-12/energy_consumption.py)
- Primary energy consumption (Our World in Data based on EI's Statistical review of world energy & EIA's International energy data):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/2024-06-20/primary_energy_consumption.py)
- Global carbon budget - Fossil CO2 emissions (Global Carbon Project):
  - [Source data](https://zenodo.org/records/13981696/files/GCB2024v17_MtCO2_flat.csv)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/gcp/2024-11-13/global_carbon_budget.py)
- Global carbon budget - Global carbon emissions (Global Carbon Project):
  - [Source data](https://globalcarbonbudgetdata.org/downloads/jGJH0-data/Global_Carbon_Budget_2024_v1.0.xlsx)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/gcp/2024-11-13/global_carbon_budget.py)
- Global carbon budget - National fossil carbon emissions (Global Carbon Project):
  - [Source data](https://globalcarbonbudgetdata.org/downloads/jGJH0-data/National_Fossil_Carbon_Emissions_2024v1.0.xlsx)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/gcp/2024-11-13/global_carbon_budget.py)
- Global carbon budget - National land-use change carbon emissions (Global Carbon Project):
  - [Source data](https://globalcarbonbudgetdata.org/downloads/jGJH0-data/National_LandUseChange_Carbon_Emissions_2024v1.0.xlsx)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/gcp/2024-11-13/global_carbon_budget.py)
- Global carbon budget (Our World in Data based on the Global Carbon Project's Fossil CO2 emissions, Global carbon emissions, National fossil carbon emissions, and National land-use change emissions):
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/gcp/2024-11-13/global_carbon_budget.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/gcp/2024-11-13/global_carbon_budget.py)
- National contributions to climate change (Jones et al. (2024)):
  - [Source data](https://zenodo.org/records/7636699/latest)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/emissions/2024-11-21/national_contributions.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/emissions/2024-11-21/national_contributions.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/emissions/2024-11-21/national_contributions.py)
- CO2 dataset (Our World in Data based on all sources above):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/emissions/2024-11-21/owid_co2.py)
  - [Exporting code](https://github.com/owid/etl/blob/master/etl/steps/export/github/co2_data/latest/owid_co2.py)
  - [Uploading code](https://github.com/owid/etl/blob/master/etl/steps/export/s3/co2_data/latest/owid_co2.py)

Additionally, to construct indicators per capita and per GDP, we use the following datasets and processing steps:
- Regions (Our World in Data).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/regions/2023-01-01/regions.py)
- Population (Our World in Data based on [a number of different sources](https://ourworldindata.org/population-sources)).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/demography/2024-07-15/population/__init__.py)
- Income groups (World Bank).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/wb/2024-07-29/income_groups.py)
- GDP (University of Groningen GGDC's Maddison Project Database, Bolt and van Zanden, 2024).
  - [Source data](https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/ggdc/2024-04-26/maddison_project_database.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/ggdc/2024-04-26/maddison_project_database.py)
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/ggdc/2024-04-26/maddison_project_database.py)

## Changelog

- 2024-11-21:
  - Updated dataset (and codebook) to use the latest version of the Global Carbon Budget (2024), and Jones et al. (2024) (version 2024.2).
  - Now methane, nitrous oxide, and total greenhouse gas emissions data come from Jones et al. (2024), instead of Climate Watch, to provide a wider data coverage.
- 2024-06-20:
  - Update data from the Statistical Review of World Energy.
  - Update data from the Maddison Project Database.
- 2024-04-10:
  - Updated dataset and codebook to use the latest version of the data on National contributions to climate change (Jones et al. (2024)).
- 2023-12-28:
  - Enhanced codebook (improved descriptions, added units, updated sources).
  - Updated primary energy consumption (to update metadata, nothing has changed in the data).
- 2023-12-05:
  - Updated dataset (and codebook) to use the latest version of the Global Carbon Budget (2023).
    - In this version, "International transport" has been replaced by "International aviation" and "International shipping". Also, some overseas territories have no data in this version. More details on the changes can be found in the pdf file hosted [here](https://zenodo.org/records/10177738).
- 2023-11-08:
  - Updated CO2 emissions data to use the latest emissions by sector from Climate Watch (2023).
  - Update codebook accordingly.
- 2023-10-16:
  - Improved codebook.
  - Fixed issue related to consumption-based emissions in Africa, and Palau emissions.
- 2023-07-10:
  - Updated primary energy consumption and other indicators relying on energy data, to use the latest Statistical Review of World Energy by the Energy Institute.
  - Renamed countries 'East Timor' and 'Faroe Islands'.
- 2023-05-04:
  - Added indicators `share_of_temperature_change_from_ghg`, `temperature_change_from_ch4`, `temperature_change_from_co2`, `temperature_change_from_ghg`, and `temperature_change_from_n2o` using data from Jones et al. (2023).
- 2022-11-11:
  - Updated CO2 emissions data with the newly released Global Carbon Budget (2022) by the Global Carbon Project.
  - Added various new indicators related to national land-use change emissions.
  - Added the emissions of the 1991 Kuwaiti oil fires in Kuwait's emissions (while also keeping 'Kuwaiti Oil Fires (GCP)' as a separate entity), to properly account for these emissions in the aggregate of Asia.
  - Applied minor changes to entity names (e.g. "Asia (excl. China & India)" -> "Asia (excl. China and India)").
- 2022-09-06:
  - Updated data on primary energy consumption (from BP & EIA) and greenhouse gas emissions by sector (from CAIT).
  - Refactored code, since now this repository simply loads the data, generates the output files, and uploads them to the cloud; the code to generate the dataset is now in our [etl repository](https://github.com/owid/etl).
  - Minor changes in the codebook.
- 2022-04-15:
  - Updated primary energy consumption data.
  - Updated CO2 data to include aggregations for the different country income levels.
- 2022-02-24:
  - Updated greenhouse gas emissions data from CAIT Climate Data Explorer.
  - Included two new columns in dataset: total greenhouse gases excluding land-use change and forestry, and the same as per capita values.
- 2021-11-05: Updated CO2 emissions data with the newly released Global Carbon Budget (v2021).
- 2021-09-16:
  - Fixed data quality issues in CO2 emissions indicators (emissions less than 0, missing data for Eswatini, ...).
  - Replaced all input CSVs with data retrieved directly from ourworldindata.org.
- 2021-02-08: Updated this dataset with the latest annual release from the Global Carbon Project.
- 2020-08-07: The first version of this dataset was made available.

## Data alterations

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names in order to minimize data loss during data merges.
- **We recalculate carbon emissions to CO2.** The primary data sources on CO2 emissions—the Global Carbon Project, for example—typically report emissions in tonnes of carbon. We have recalculated these figures as tonnes of CO2 using a conversion factor of 3.664.
- **We calculate per capita figures.** All of our per capita figures are calculated from our metric `Population`, which is included in the complete dataset. These population figures are sourced from [Gapminder](http://gapminder.org) and the [UN World Population Prospects (UNWPP)](https://population.un.org/wpp/).

## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.

## Authors

This data has been collected, aggregated, and documented by Hannah Ritchie, Max Roser, Edouard Mathieu, Bobbie Macdonald and Pablo Rosado.

The mission of *Our World in Data* is to make data and research on the world's largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).


## How to cite this data?

If you are using this dataset, please cite both [Our World in Data](https://ourworldindata.org/co2-and-greenhouse-gas-emissions#article-citation) and the underlying data source(s).

Please follow [the guidelines in our FAQ](https://ourworldindata.org/faqs#citing-work-produced-by-third-parties-and-made-available-by-our-world-in-data) on how to cite our work.

