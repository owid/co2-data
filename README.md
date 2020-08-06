# Data on CO2 and Greenhouse Gas Emissions by *Our World in Data*

Our complete CO2 and Greenhouse Gas Emissions dataset is a collection of key metrics maintained by [*Our World in Data*](http://ourworldindata.org/co2-and-greenhouse-gas-emissions). It is updated regularly and includes data on CO2 emissions (annual, per capita, cumulative and consumption-based), other greenhouse gases, energy mix, and other relevant metrics.

### 🗂️ Download our complete CO2 and Greenhouse Gas Emissions dataset : [CSV](#) | [XLSX](#) | [JSON](#)

We will continue to publish updated data on CO2 and Greenhouse Gas Emissions as it becomes available. Most metrics are published on an annual basis.

## Our data sources

- **CO2 emissions**: we have combined data from two sources: the [Global Carbon Project](http://www.globalcarbonproject.org/carbonbudget) and the [Carbon Dioxide Information Analysis Center](https://cdiac.ess-dive.lbl.gov/trends/emis/meth_reg.html) (CDIAC). Data until the year 1959 is taken from CDIAC; data from 1959 onwards is sourced from the Global Carbon Project. The Global Carbon Project typically releases a new update of CO2 emissions annually.
- **Greenhouse gas emissions (including methane, and nitrous oxide):** this data is sourced from the CAIT Climate Data Explorer, and downloaded from the [Climate Watch Portal](https://www.climatewatchdata.org/data-explorer/historical-emissionshttps://www.climatewatchdata.org/data-explorer/historical-emissions).
- **Energy (primary energy, energy mix and energy intensity):** this data is sourced from a combination of two sources. The [BP Statistical Review of World Energy](https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html) is published annually, but it does not provide data on primary energy consumption for all countries. For countries absent from this dataset, we calculate primary energy by multiplying the [World Bank, World Development Indicators](https://databank.worldbank.org/source/world-development-indicators) metric 'Energy use per capita' by total population figures. The World Bank sources this metric from the IEA.
- **Other variables:** this data is collected from a variety of sources (United Nations, World Bank, Gapminder, Maddison Project Database, etc.). More information is available in [our codebook](#).

## The complete *Our World in Data* CO2 and Greenhouse Gas Emissions dataset

**Our complete CO2 and Greenhouse Gas Emissions dataset is available in [CSV](#), [XLSX](#), and [JSON](#) formats.**

The CSV and XLSX files follow a format of 1 row per location and year. The JSON version is split by country ISO code, with static variables and an array of yearly records.

The variables represent all of our main data related to CO2 emissions, other greenhouse gas emissions, energy mix, as well as other variables of potential interest.

A [full codebook](#) is made available, with a description and source for each variable in the dataset.

## Changelog

## Data alterations

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names to the [*Our World in Data* standard entity names](#).
- **We recalculate carbon emissions to CO2.** The primary data sources on CO2 emissions—the Global Carbon Project, for example—typically report emissions in tonnes of carbon. We have recalculated these figures as tonnes of CO2 using a conversion factor of 3.664.
- **We calculate per capita figures.** All of our per capita figures are calculated from our metric `Population`, which is included in the complete dataset. These population figures are sourced from [Gapminder](http://gapminder.org) and the [UN World Population Prospects (UNWPP)](https://population.un.org/wpp/).

## License

All of *Our World in Data* is completely open access and all work is licensed under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce in any medium, provided the source and authors are credited.

## Authors

This data has been collected, aggregated, and documented by Hannah Ritchie, Max Roser and Edouard Mathieu.

The mission of *Our World in Data* is to make data and research on the world’s largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).
