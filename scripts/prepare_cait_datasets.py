"""Download data from CAIT using Climate Watch Data API, process data, and export a csv file.

See https://www.climatewatchdata.org/data-explorer/historical-emissions

"""

import argparse
import json
import os
from time import sleep

import pandas as pd
import requests
from owid import catalog
from tqdm.auto import tqdm

# Define paths.
CURRENT_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(CURRENT_DIR, "input")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "grapher")
# CAIT API URL.
CAIT_API_URL = "https://www.climatewatchdata.org/api/v1/data/historical_emissions/"
# Number of records to fetch per api request.
API_RECORDS_PER_REQUEST = 500
# Time to wait between consecutive api requests.
TIME_BETWEEN_REQUESTS = 0.1
# Temporary file for API data.
TEMP_API_DATA_FILE = os.path.join(OUTPUT_DIR, "temp.json")

# Define default naming conventions (some of which will be in the outputs).
NAME = {
    'country': 'Country',
    'data_source': 'Data source',
    'gas': 'Gas',
    'iso_3': 'ISO alpha 3',
    'population': 'Population',
    'sector': 'Sector',
    'value': 'Value',
    'year': 'Year',
    'sector_agriculture': 'Agriculture',
    'sector_aviation_and_shipping': 'Aviation and shipping',
    'sector_buildings': 'Buildings',
    'sector_electricity_and_heat': 'Electricity and heat',
    'sector_energy': 'Energy',
    'sector_fugitive_emissions': 'Fugitive emissions',
    'sector_industry': 'Industry',
    'sector_lucf': 'Land-use change and forestry',
    'sector_manufacturing_and_construction': 'Manufacturing and construction',
    'sector_other_fuel_combustion': 'Other fuel combustion',
    'sector_total_excluding_lucf': 'Total excluding LUCF',
    'sector_total_including_lucf': 'Total including LUCF',
    'sector_transport': 'Transport',
    'sector_waste': 'Waste',
    # For regions and groups that do not have an ISO 3 code, use OWID codes.
    'iso_3_european_union': 'OWID_EUN',
    'iso_3_world': 'OWID_WRL',
    'country_european_union': 'European Union (27)',
    'emissions_all_ghg': 'All greenhouse gases',
    'emissions_co2': 'CO2',
    'emissions_ch4': 'CH4',
    'emissions_n20': 'N2O',
    'data_source_cait': 'CAIT',
}

# Define entity names for climate watch (cw) data.
NAME_CW = {
    'country': 'country',
    'data_source': 'data_source',
    'emissions': 'emissions',
    'iso_3': 'iso_code3',
    'gas': 'gas',
    'sector': 'sector',
    'value': 'value',
    'year': 'year',
    'unit': 'unit',
    'sector_agriculture': 'Agriculture',
    'sector_aviation_and_shipping': 'Bunker Fuels',
    'sector_buildings': 'Building',
    'sector_electricity_and_heat': 'Electricity/Heat',
    'sector_energy': 'Energy',
    'sector_fugitive_emissions': 'Fugitive Emissions',
    'sector_industry': 'Industrial Processes',
    'sector_lucf': 'Land-Use Change and Forestry',
    'sector_manufacturing_and_construction': 'Manufacturing/Construction',
    'sector_other_fuel_combustion': 'Other Fuel Combustion',
    'sector_total_excluding_lucf': 'Total excluding LUCF',
    'sector_total_including_lucf': 'Total including LUCF',
    'sector_transport': 'Transportation',
    'sector_waste': 'Waste',
    'data_source_cait': 'CAIT',
    'iso_3_european_union': 'EUU',
    'iso_3_world': 'WORLD',
    'emissions_all_ghg': 'All GHG',
    'emissions_co2': 'CO2',
    'emissions_ch4': 'CH4',
    'emissions_n2o': 'N2O',
    'country_european_union': 'European Union (27)',
}

# Define entity names for OWID countries_regions (cr) data.
NAME_CR = {
    'country': 'name',
    'iso_3': 'iso_alpha3',
}

# Define entity names for OWID population (pop) data.
NAME_POP = {
    'country': 'country',
    'population': 'population',
    'year': 'year',
}

# Suffix to add to the name of per capita variables.
SUFFIX_FOR_PER_CAPITA_VARIABLES = " (per capita)"

# Define output columns that will be added as per capita variables.
COLUMNS_PER_CAPITA = [column for column in list(NAME) if column.startswith('sector_')]

# Define output files for each gas.
GASES_AND_FILES = {
    'emissions_all_ghg': os.path.join(OUTPUT_DIR, "GHG Emissions by Country and Sector (CAIT, 2021).csv"),
    'emissions_co2': os.path.join(OUTPUT_DIR, "CO2 emissions by sector (CAIT, 2021).csv"),
    'emissions_ch4': os.path.join(OUTPUT_DIR, "Methane emissions by sector (CAIT, 2021).csv"),
    'emissions_n20': os.path.join(OUTPUT_DIR, "Nitrous oxide emissions by sector (CAIT, 2021).csv"),
}

# According to the documentation of the API, it is possible to select data by data_sources, gases or sectors.
# However, when I tried the api did not select properly.
# I could select by regions, but not by data_sources, gases or sectors, which would have been convenient.

# def get_names_and_ids_of_entity(entity_api_name, base_url=CAIT_API_URL):
#     # Get names and ids of entity.
#     api_url = base_url + entity_api_name
#     session = requests.Session()
#     response = session.get(url=api_url)
#     entity_data = json.loads(response.content)["data"]
#     entity_ids = {entity["name"]: entity["id"] for entity in entity_data}

#     return entity_ids

# data_sources_ids = get_names_and_ids_of_entity(entity_api_name='data_sources')
# gases_ids = get_names_and_ids_of_entity(entity_api_name='gases')
# sectors_ids = get_names_and_ids_of_entity(entity_api_name='sectors')


def fetch_all_data_from_api(api_url=CAIT_API_URL, api_records_per_request=API_RECORDS_PER_REQUEST,
                            time_between_requests=TIME_BETWEEN_REQUESTS):
    # Start requests session.
    session = requests.Session()
    # The total number of records in the database is returned on the header of each request.
    # Send a simple request to get that number.
    response = session.get(url=api_url)
    total_records = int(response.headers['total'])
    print(f"Total number of records to fetch from API: {total_records}")

    # Number of requests to ensure all pages are requested.
    total_requests = round(total_records / api_records_per_request) + 1
    # Collect all data from consecutive api requests. This could be sped up by parallelizing requests.
    data_all = []
    for page in tqdm(range(1, total_requests + 1)):
        response = session.get(url=api_url, json={"page": page, "per_page": api_records_per_request})
        new_data = json.loads(response.content)["data"]
        if len(new_data) == 0:
            print("No more data to fetch.")
            break
        data_all.extend(new_data)
        sleep(time_between_requests)

    return data_all


def get_population_dataset(name_pop=NAME_POP, name=NAME):
    # Get a population dataset from the owid catalog.
    population = catalog.find('population', namespace='owid').load().reset_index()
    # For future reference, this is the current checksum of the dataset:
    # population.metadata.dataset.source_checksum
    # 'c89210a8324b4d5f09215b988a0dc19c'

    # Select required columns and adapt entities to default naming.
    population = population[[name_pop['country'], name_pop['population'], name_pop['year']]].rename(columns={
        name_pop['country']: name['country'],
        name_pop['population']: name['population'],
        name_pop['year']: name['year'],
    })
    # Countries do not need to be adapted, since these are the country names that will be used as default.

    return population


def get_countries_regions_dataset(name_cr=NAME_CR, name=NAME):
    # We also need a dataset that translates country names from iso 3 to owid country names.
    countries_regions = catalog.find('countries_regions', namespace='owid').load().reset_index()

    # Fill missing ISO codes with OWID codes.
    countries_regions['iso_alpha3'] = countries_regions['iso_alpha3'].fillna(countries_regions['code'])

    # Standardize columns.
    countries_regions = countries_regions[[name_cr['country'], name_cr['iso_3']]].rename(columns={
        name_cr['country']: name['country'],
        name_cr['iso_3']: name['iso_3'],
    })

    return countries_regions


def prepare_ghg_data(api_data, name_cw=NAME_CW, name=NAME):
    columns_to_keep = [column for column in list(api_data[0]) if column != name_cw['emissions']]

    # Create dataframe of GHG data.
    ghg_data = pd.json_normalize(api_data, record_path=[name_cw['emissions']], meta=columns_to_keep)

    # Standardize columns.
    ghg_data = ghg_data[[
        name_cw['year'], name_cw['iso_3'], name_cw['data_source'], name_cw['sector'], name_cw['gas'], name_cw['value'],
    ]].rename(columns={
        name_cw['year']: name['year'],
        name_cw['iso_3']: name['iso_3'],
        name_cw['sector']: name['sector'],
        name_cw['gas']: name['gas'],
        name_cw['data_source']: name['data_source'],
        name_cw['value']: name['value'],
    })
    # Standardize rows.
    ghg_data[name['sector']] = ghg_data[name['sector']].replace({
        name_cw['sector_agriculture']: name['sector_agriculture'],
        name_cw['sector_aviation_and_shipping']: name['sector_aviation_and_shipping'],
        name_cw['sector_buildings']: name['sector_buildings'],
        name_cw['sector_electricity_and_heat']: name['sector_electricity_and_heat'],
        name_cw['sector_energy']: name['sector_energy'],
        name_cw['sector_fugitive_emissions']: name['sector_fugitive_emissions'],
        name_cw['sector_industry']: name['sector_industry'],
        name_cw['sector_lucf']: name['sector_lucf'],
        name_cw['sector_manufacturing_and_construction']: name['sector_manufacturing_and_construction'],
        name_cw['sector_other_fuel_combustion']: name['sector_other_fuel_combustion'],
        name_cw['sector_total_excluding_lucf']: name['sector_total_excluding_lucf'],
        name_cw['sector_total_including_lucf']: name['sector_total_including_lucf'],
        name_cw['sector_transport']: name['sector_transport'],
        name_cw['sector_waste']: name['sector_waste'],
    })
    ghg_data[name['data_source']] = ghg_data[name['data_source']].replace({
        name_cw['data_source_cait']: name['data_source_cait'],
    })
    ghg_data[name['gas']] = ghg_data[name['gas']].replace({
        name_cw['emissions_all_ghg']: name['emissions_all_ghg'],
        name_cw['emissions_co2']: name['emissions_co2'],
        name_cw['emissions_ch4']: name['emissions_ch4'],
        name_cw['emissions_n2o']: name['emissions_n20'],
    })
    ghg_data[name['iso_3']] = ghg_data[name['iso_3']].replace({
        name_cw['iso_3_european_union']: name['iso_3_european_union'],
        name_cw['iso_3_world']: name['iso_3_world'],
    })

    # Select CAIT data.
    ghg_data = ghg_data[(ghg_data[name['data_source']] == name['data_source_cait'])].reset_index(drop=True).\
        drop(columns=[name['data_source']])

    return ghg_data


def combine_ghg_and_population(ghg_data, population, name=NAME):
    # Add population and standardized country names to GHG data.
    ghg_data = pd.merge(ghg_data, population, on=(name['iso_3'], name['year']), how='left').\
        drop(columns=[name['iso_3']])

    # Check if there are missing countries or populations.
    missing_countries = ghg_data[
        (ghg_data[name['country']].isnull()) | (ghg_data[name['population']].isnull())][name['country']].\
        unique().tolist()

    error_message = f"Missing population data for {', '.join(missing_countries)}."
    assert len(missing_countries) == 0, error_message

    return ghg_data


def collect_data_for_each_gas(ghg_data, name=NAME, gases_and_files=GASES_AND_FILES,
                              columns_per_capita=COLUMNS_PER_CAPITA,
                              suffix_for_per_capita_variables=SUFFIX_FOR_PER_CAPITA_VARIABLES):
    data_for_gases = {}
    # Export a dataset for all ghg and for each individual gas.
    for gas in gases_and_files:
        # Select data for this gas, and create a column for each sector.
        gas_data = ghg_data.loc[ghg_data[name['gas']] == name[gas]].\
                    pivot_table(index=[name['country'], name['year'], name['population']], columns=name['sector'],
                                values=name['value']).reset_index()

        # Add per capita metrics.
        for col in columns_per_capita:
            if name[col] in gas_data.columns:
                # Convert megatons (of co2 equivalents) to tons, and divide by population, to get tons per capita.
                gas_data[name[col] + suffix_for_per_capita_variables] =\
                    gas_data[name[col]] / gas_data[name['population']] * 1e6
        gas_data = gas_data.drop(columns=[name['population']])

        # Sort columns more conveniently.
        left_columns = [name['country'], name['year']]
        other_columns = sorted([col for col in gas_data.columns if col not in left_columns])
        column_order = left_columns + other_columns
        gas_data = gas_data[column_order]

        # Add data for this gas to complete dictionary.
        data_for_gases[gas] = gas_data

    return data_for_gases


def save_data_for_each_gas(data_for_gases, gases_and_files=GASES_AND_FILES):
    for gas in gases_and_files:
        # Save gas data to output file.
        output_file = gases_and_files[gas]
        print(f"Saving data to file: {output_file}")
        data_for_gases[gas].to_csv(output_file, index=False)


########################################################################################################################
# TODO: Remove this temporary solution once EU 27 is added to population dataset.
def add_eu_27_to_population_dataset(population, name=NAME):
    eu_countries_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/scripts/input/owid/eu_countries.csv"
    eu_countries = pd.read_csv(eu_countries_url)
    eu_population = population[population[name['country']].isin(eu_countries['Country'].tolist())].\
        reset_index(drop=True).groupby(name['year']).\
        agg({name['country']: lambda x: len(list(x)), name['population']: sum}).reset_index()
    # Check that there are indeed 27 countries each year.
    # This is historically inaccurate, but we assume the EU corresponds to its current state.
    assert all(eu_population[name['country']] == 27)
    # Add the EU to population dataframe.
    eu_population[name['country']] = name['country_european_union']
    eu_population[name['iso_3']] = name['iso_3_european_union']
    population = pd.concat([population, eu_population], ignore_index=True)

    return population
########################################################################################################################


########################################################################################################################
# TODO: Remove this temporary solution once Cook Islands and Niue are added to population dataset.
def add_population_for_missing_countries(population, countries_regions, name=NAME):
    missing_countries = ['Cook Islands', 'Niue']
    # Download complete population dataset.
    complete_population_url = "https://github.com/owid/owid-datasets/raw/master/datasets/Population%20(Gapminder" \
                              "%2C%20HYDE%20%26%20UN)/Population%20(Gapminder%2C%20HYDE%20%26%20UN).csv"
    complete_population = pd.read_csv(complete_population_url)
    # Standardize column names.
    complete_population = complete_population.rename(columns={
        'Population (historical estimates and future projections)': name['population'],
        'Entity': name['country'],
        'Year': name['year'],
    })[[name['country'], name['population'], name['year']]]
    # Add ISO 3 code.
    complete_population = pd.merge(complete_population, countries_regions, on=name['country'], how='left')
    # Add missing countries to population dataframe.
    population = pd.concat(
        [population, complete_population[complete_population[name['country']].isin(missing_countries)]],
        ignore_index=True)

    return population
########################################################################################################################


def main(name=NAME, temp_api_data_file=TEMP_API_DATA_FILE):
    # Download all data from api (since we cannot select by data sources, gases or sectors).
    # It took about 3 minutes for 500 records and 0.1 seconds sleep per request.

    if os.path.isfile(temp_api_data_file):
        print(f"Loading Climate Watch Data from local file: {temp_api_data_file}")
        with open(temp_api_data_file) as _json_file:
            api_data = json.load(_json_file)
    else:
        print("Loading data from the Climate Watch Data API.")
        api_data = fetch_all_data_from_api()
        print(f"Saving API data to temporary file: {temp_api_data_file}")
        with open(temp_api_data_file, "w") as _json_file:
            json.dump(api_data, _json_file)

    print("Load OWID population data.")
    population = get_population_dataset()
    countries_regions = get_countries_regions_dataset()

    # Add ISO 3 code to population dataset.
    population = pd.merge(population, countries_regions, on=name['country'], how='left')

    ####################################################################################################################
    # TODO: Remove this temporary solution once EU 27 is added to population dataset.
    population = add_eu_27_to_population_dataset(population=population)
    ####################################################################################################################

    ####################################################################################################################
    # TODO: Remove this temporary solution once Cook Islands and Niue are added to population dataset.
    population = add_population_for_missing_countries(population=population, countries_regions=countries_regions)
    ####################################################################################################################

    print("Prepare data on greenhouse gases.")
    ghg_data = prepare_ghg_data(api_data)

    # Combine GHG and population data.
    ghg_data = combine_ghg_and_population(ghg_data, population, name=NAME)

    # Collect data for each relevant type of emission.
    data_for_gases = collect_data_for_each_gas(ghg_data=ghg_data)

    print("Saving data in a file for each relevant type of emission.")
    save_data_for_each_gas(data_for_gases)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and prepare datasets on GHG data from different sectors."
    )
    args = parser.parse_args()

    main()
