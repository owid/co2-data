# Data on CO2 and Greenhouse Gas Emissions by *Our World in Data*

Our complete CO2 and Greenhouse Gas Emissions dataset is a collection of key metrics maintained by [*Our World in Data*](https://ourworldindata.org/co2-and-other-greenhouse-gas-emissions). It is updated regularly and includes data on CO2 emissions (annual, per capita, cumulative and consumption-based), other greenhouse gases, energy mix, and other relevant metrics.

### 🗂️ Download our complete CO2 and Greenhouse Gas Emissions dataset : [CSV](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv) | [XLSX](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.xlsx) | [JSON](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.json)

We will continue to publish updated data on CO2 and Greenhouse Gas Emissions as it becomes available. Most metrics are published on an annual basis.

## Our data sources

- **CO2 emissions**: this data is sourced from the [Global Carbon Project](http://www.globalcarbonproject.org/carbonbudget). The Global Carbon Project typically releases a new update of CO2 emissions annually.
- **Greenhouse gas emissions (including methane, and nitrous oxide):** this data is sourced from the CAIT Climate Data Explorer, and downloaded from the [Climate Watch Portal](https://www.climatewatchdata.org/data-explorer/historical-emissions).
- **Energy (primary energy, energy mix and energy intensity):** this data is sourced from a combination of two sources. The [BP Statistical Review of World Energy](https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html) is published annually, but it does not provide data on primary energy consumption for all countries. For countries absent from this dataset, we calculate primary energy by multiplying the [World Bank, World Development Indicators](https://databank.worldbank.org/source/world-development-indicators) metric `Energy use per capita` by total population figures. The World Bank sources this metric from the IEA.
- **Other variables:** this data is collected from a variety of sources (United Nations, World Bank, Gapminder, Maddison Project Database, etc.). More information is available in [our codebook](https://github.com/owid/co2-data/blob/master/owid-co2-codebook.csv).

## The complete *Our World in Data* CO2 and Greenhouse Gas Emissions dataset

**Our complete CO2 and Greenhouse Gas Emissions dataset is available in [CSV](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv), [XLSX](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.xlsx), and [JSON](https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.json) formats.**

The CSV and XLSX files follow a format of 1 row per location and year. The JSON version is split by country, with an array of yearly records.

The variables represent all of our main data related to CO2 emissions, other greenhouse gas emissions, energy mix, as well as other variables of potential interest.

A [full codebook](https://github.com/owid/co2-data/blob/master/owid-co2-codebook.csv) is made available, with a description and source for each variable in the dataset.

## Changelog

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
