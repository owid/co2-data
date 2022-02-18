"""Download data from CAIT using Climate Watch Data API, process data, and export a csv file.

See https://www.climatewatchdata.org/data-explorer/historical-emissions

"""

import argparse
import os
import webbrowser
from datetime import datetime

import pandas as pd

import sanity_checks

# TODO: Checks to do:
#  * Check that
#    data[['Buildings', 'Electricity and heat', 'Fugitive emissions', 'Manufacturing and construction',
#         'Other fuel combustion', 'Transport']].sum(axis=1)
#    and check['Energy'] are almost identical.

# Define paths.
CURRENT_DIR = os.path.dirname(__file__)
DATASET_DIR = os.path.join(CURRENT_DIR, "grapher")

# Date tag and output file for visual inspection of potential issues with the dataset.
DATE_TAG = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(CURRENT_DIR, f"cait_datasets_sanity_checks_{DATE_TAG}.html")

# Define parameters for output figures.

# Label to use for old dataset.
DATA_LABEL_OLD = "old"
# Label to use for new dataset.
DATA_LABEL_NEW = "new"
# True to include interactive plots in output HTML file (which can make the inspection slow if there are many figures);
# False to include plots as static images.
EXPORT_INTERACTIVE_PLOTS = False
# Maximum number of plots (of potentially problematic cases) to show in output file.
MAX_NUM_PLOTS = 150


DATASET_NEW_FILES = {
    'emissions_all_ghg': os.path.join(DATASET_DIR, "GHG Emissions by Country and Sector (CAIT, 2021).csv"),
    'emissions_co2': os.path.join(DATASET_DIR, "CO2 emissions by sector (CAIT, 2021).csv"),
    'emissions_ch4': os.path.join(DATASET_DIR, "Methane emissions by sector (CAIT, 2021).csv"),
    'emissions_n20': os.path.join(DATASET_DIR, "Nitrous oxide emissions by sector (CAIT, 2021).csv"),
}

# Define URL of previous datasets for each gas (for checking purposes).
# TODO: Datasets should be directly loaded from owid.catalog instead of git.
OWID_DATASETS_URL = "https://github.com/owid/owid-datasets/raw/master/datasets/"
DATASET_OLD_FILES = {
    'emissions_all_ghg': OWID_DATASETS_URL + "GHG%20Emissions%20by%20Country%20and%20Sector%20(CAIT%2C%202020)/GHG%20"
                                             "Emissions%20by%20Country%20and%20Sector%20(CAIT%2C%202020).csv",
    'emissions_co2': OWID_DATASETS_URL + "CO2%20emissions%20by%20sector%20(CAIT%2C%202020)/CO2%20emissions%20by%20"
                                         "sector%20(CAIT%2C%202020).csv",
    'emissions_ch4': OWID_DATASETS_URL + "Methane%20emissions%20by%20sector%20(CAIT%2C%202020)/Methane%20emissions%20"
                                         "by%20sector%20(CAIT%2C%202020).csv",
    'emissions_n20': OWID_DATASETS_URL + "Nitrous%20oxide%20emissions%20by%20sector%20(CAIT%2C%202020)/Nitrous%20oxide"
                                         "%20emissions%20by%20sector%20(CAIT%2C%202020).csv",
}

NAME_ALL_OLD = {
    'country': 'Entity',
    'year': 'Year',
    # Total values.
    'agriculture': 'Agriculture (GHG Emissions, CAIT)',
    'aviation_and_shipping': 'Bunker Fuels (GHG Emissions, CAIT)',
    'buildings': 'Buildings (GHG Emissions, CAIT)',
    'electricity_and_heat': 'Electricity & Heat (GHG Emissions, CAIT)',
    'energy': 'Energy (GHG Emissions, CAIT)',
    'fugitive_emissions': 'Fugitive from energy production (GHG Emissions, CAIT)',
    'industry': 'Industry (GHG Emissions, CAIT)',
    'lucf': 'Land-Use Change and Forestry (GHG Emissions, CAIT)',
    'manufacturing_and_construction': 'Manufacturing/Construction energy (GHG Emissions, CAIT)',
    'other_fuel_combustion': 'Other Fuel Combustion (GHG Emissions, CAIT)',
    'total_excluding_lucf': 'Total GHG emissions excluding LUCF (CAIT)',
    'total_including_lucf': 'Total GHG emissions including LUCF (CAIT)',
    'transport': 'Transport (GHG Emissions, CAIT)',
    'waste': 'Waste (GHG Emissions, CAIT)',
    # Per capita values.
    'agriculture_per_capita': 'Agriculture (per capita) (GHG Emissions, CAIT)',
    'aviation_and_shipping_per_capita': 'International aviation & shipping (per capita) (GHG Emissions, CAIT)',
    'buildings_per_capita': 'Buildings (per capita) (GHG Emissions, CAIT)',
    'electricity_and_heat_per_capita': 'Electricity & Heat (per capita) (GHG Emissions, CAIT)',
    # "Energy" had no per capita value in the 2020 dataset.
    # 'energy_per_capita': 'Energy (per capita) (GHG Emissions, CAIT)',
    'fugitive_emissions_per_capita': 'Fugitive Emissions (per capita) (GHG Emissions, CAIT)',
    'industry_per_capita': 'Industry (per capita) (GHG Emissions, CAIT)',
    'lucf_per_capita': 'Land-Use Change and Forestry (per capita) (GHG Emissions, CAIT)',
    'manufacturing_and_construction_per_capita': 'Manufacturing & Construction (per capita) (GHG Emissions, CAIT)',
    # "Other fuel combustion" had no per capita value in the 2020 dataset.
    # 'other_fuel_combustion_per_capita': 'Other Fuel Combustion (per capita) (GHG Emissions, CAIT)',
    'total_excluding_lucf_per_capita': 'Total excluding LUCF (per capita) (GHG Emissions, CAIT)',
    'total_including_lucf_per_capita': 'Total including LUCF (per capita) (GHG Emissions, CAIT)',
    'transport_per_capita': 'Transport (per capita) (GHG Emissions, CAIT)',
    'waste_per_capita': 'Waste (per capita) (GHG Emissions, CAIT)',
}

NAME_CO2_OLD = {
    'country': 'Entity',
    'year': 'Year',
    'aviation_and_shipping': "Int'l aviation & shipping (CAIT)",
    'buildings': 'Building (CAIT)',
    'electricity_and_heat': 'Electricity & Heat (CAIT)',
    'energy': 'Energy (CAIT)',
    'fugitive_emissions': 'Fugitive Emissions (CAIT)',
    'industry': 'Industry (CAIT)',
    'lucf': 'Land-Use Change and Forestry (CAIT)',
    'manufacturing_and_construction': 'Manufacturing & Construction (CAIT)',
    'other_fuel_combustion': 'Other Fuel Combustion (CAIT)',
    'total_excluding_lucf': 'Total excluding LUCF (CAIT)',
    'total_including_lucf': 'Total including LUCF (CAIT)',
    'transport': 'Transport (CAIT)',
    'aviation_and_shipping_per_capita': 'International aviation & shipping (per capita) (CAIT)',
    'buildings_per_capita': 'Buildings (per capita) (CAIT)',
    'electricity_and_heat_per_capita': 'Electricity & Heat (per capita) (CAIT)',
    'fugitive_emissions_per_capita': 'Fugitive Emissions (per capita) (CAIT)',
    'industry_per_capita': 'Industry (per capita) (CAIT)',
    'lucf_per_capita': 'Land-Use Change and Forestry (per capita) (CAIT)',
    'manufacturing_and_construction_per_capita': 'Manufacturing & Construction (per capita) (CAIT)',
    'total_excluding_lucf_per_capita': 'Total excluding LUCF (per capita) (CAIT)',
    'total_including_lucf_per_capita': 'Total including LUCF (per capita) (CAIT)',
    'transport_per_capita': 'Transport (per capita) (CAIT)',
}

NAME_CH4_OLD = {
    'country': 'Entity',
    'year': 'Year',
    'agriculture': 'Agriculture (CH4 emissions, CAIT)',
    'energy': 'Energy (CH4 emissions, CAIT)',
    'fugitive_emissions': 'Fugitive Emissions (CH4 emissions, CAIT)',
    'industry': 'Industry (CH4 emissions, CAIT)',
    'lucf': 'Land-Use Change and Forestry (CH4 emissions, CAIT)',
    'other_fuel_combustion': 'Other Fuel Combustion (CH4 emissions, CAIT)',
    'total_excluding_lucf': 'Total excluding LUCF (CH4 emissions, CAIT)',
    'total_including_lucf': 'Total including LUCF (CH4 emissions, CAIT)',
    'waste': 'Waste (CH4 emissions, CAIT)',
    'agriculture_per_capita': 'Agriculture (per capita) (CH4 emissions, CAIT)',
    'fugitive_emissions_per_capita': 'Fugitive Emissions (per capita) (CH4 emissions, CAIT)',
    'industry_per_capita': 'Industry (per capita) (CH4 emissions, CAIT)',
    'lucf_per_capita': 'Land-Use Change and Forestry (per capita) (CH4 emissions, CAIT)',
    'total_excluding_lucf_per_capita': 'Total excluding LUCF (per capita) (CH4 emissions, CAIT)',
    'total_including_lucf_per_capita': 'Total including LUCF (per capita) (CH4 emissions, CAIT)',
    'waste_per_capita': 'Waste (per capita) (CH4 emissions, CAIT)',
}

NAME_N2O_OLD = {
    'country': 'Entity',
    'year': 'Year',
    'agriculture': 'Agriculture (N2O emissions, CAIT)',
    'energy': 'Energy (N2O emissions, CAIT)',
    'fugitive_emissions': 'Fugitive Emissions (N2O emissions, CAIT)',
    'industry': 'Industry (N2O emissions, CAIT)',
    'lucf': 'Land-Use Change and Forestry (N2O emissions, CAIT)',
    'other_fuel_combustion': 'Other Fuel Combustion (N2O emissions, CAIT)',
    'total_excluding_lucf': 'Total excluding LUCF (N2O emissions, CAIT)',
    'total_including_lucf': 'Total including LUCF (N2O emissions, CAIT)',
    'waste': 'Waste (N2O emissions, CAIT)',
    'agriculture_per_capita': 'Agriculture (per capita) (CAIT)',
    'fugitive_emissions_per_capita': 'Fugitive Emissions (per capita) (CAIT)',
    'industry_per_capita': 'Industry (per capita) (CAIT)',
    'lucf_per_capita': 'Land-Use Change and Forestry (per capita) (CAIT)',
    'total_excluding_lucf_per_capita': 'Total excluding LUCF (per capita) (CAIT)',
    'total_including_lucf_per_capita': 'Total including LUCF (per capita)',
    'waste_per_capita': 'Waste (per capita) (CAIT)',
}

NAME_ALL_NEW = {
    'country': 'Country',
    'year': 'Year',
    # Total values.
    'agriculture': 'Agriculture',
    'aviation_and_shipping': 'Aviation and shipping',
    'buildings': 'Buildings',
    'electricity_and_heat': 'Electricity and heat',
    'energy': 'Energy',
    'fugitive_emissions': 'Fugitive emissions',
    'industry': 'Industry',
    'lucf': 'Land-use change and forestry',
    'manufacturing_and_construction': 'Manufacturing and construction',
    'other_fuel_combustion': 'Other fuel combustion',
    'total_excluding_lucf': 'Total excluding LUCF',
    'total_including_lucf': 'Total including LUCF',
    'transport': 'Transport',
    'waste': 'Waste',
    # Per capita values.
    'agriculture_per_capita': 'Agriculture (per capita)',
    'aviation_and_shipping_per_capita': 'Aviation and shipping (per capita)',
    'buildings_per_capita': 'Buildings (per capita)',
    'electricity_and_heat_per_capita': 'Electricity and heat (per capita)',
    'energy_per_capita': 'Energy (per capita)',
    'fugitive_emissions_per_capita': 'Fugitive emissions (per capita)',
    'industry_per_capita': 'Industry (per capita)',
    'lucf_per_capita': 'Land-use change and forestry (per capita)',
    'manufacturing_and_construction_per_capita': 'Manufacturing and construction (per capita)',
    'other_fuel_combustion_per_capita': 'Other fuel combustion (per capita)',
    'total_excluding_lucf_per_capita': 'Total excluding LUCF (per capita)',
    'total_including_lucf_per_capita': 'Total including LUCF (per capita)',
    'transport_per_capita': 'Transport (per capita)',
    'waste_per_capita': 'Waste (per capita)',
}

NAME_CO2_NEW = {col for col in NAME_ALL_NEW if col not in [
    'Agriculture', 'Agriculture (per capita)', 'Waste', 'Waste (per capita)']}
NAME_CH4_NEW = {col for col in NAME_ALL_NEW if col not in [
    'Buildings', 'Buildings (per capita)', 'Electricity and heat', 'Electricity and heat (per capita)',
    'International aviation and shipping', 'International aviation and shipping (per capita)',
    'Manufacturing and construction', 'Manufacturing and construction (per capita)', 'Transport',
    'Transport (per capita)']}
NAME_N2O_NEW = {col for col in NAME_ALL_NEW if col not in [
    'Buildings', 'Buildings (per capita)', 'Electricity and heat', 'Electricity and heat (per capita)',
    'International aviation and shipping', 'International aviation and shipping (per capita)',
    'Manufacturing and construction', 'Manufacturing and construction (per capita)', 'Transport',
    'Transport (per capita)']}

# Define default entity names.
NAME = NAME_ALL_NEW.copy()

# For convenience, put all dictionaries together keyed by dataset name.
NAME_OLD = {
    'emissions_all_ghg': NAME_ALL_OLD,
    'emissions_co2': NAME_CO2_OLD,
    'emissions_ch4': NAME_CH4_OLD,
    'emissions_n2o': NAME_N2O_OLD,
}
NAME_NEW = {
    'emissions_all_ghg': NAME_ALL_NEW,
    'emissions_co2': NAME_CO2_NEW,
    'emissions_ch4': NAME_CH4_NEW,
    'emissions_n2o': NAME_N2O_NEW,
}

# Define entity names for OWID population dataset.
# TODO: Use this once OWID population from catalog is updated.
# NAME_POPULATION = {
#     "country": "country",
#     "year": "year",
# }
# TODO: Remove this once OWID population from catalog is updated.
NAME_POPULATION = {
    "country": "Entity",
    "year": "Year",
}

# Latest possible value for the minimum year.
MIN_YEAR_LATEST_POSSIBLE = 2000
# Maximum delay (from current year) that maximum year can have.
MAX_YEAR_MAXIMUM_DELAY = 4

MIN_EMISSIONS = 0
MAX_EMISSIONS = 'World'
MIN_EMISSIONS_PER_CAPITA = 0
MAX_EMISSIONS_PER_CAPITA = 100
MIN_RELEVANT_EMISSIONS = 10

# TODO: Choose meaningful ranges.
RANGES = {
    # Total values.
    'agriculture': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'aviation_and_shipping': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'buildings': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'electricity_and_heat': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'energy': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'fugitive_emissions': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'industry': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'lucf': {
        'min': -100,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'manufacturing_and_construction': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'other_fuel_combustion': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'total_excluding_lucf': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'total_including_lucf': {
        'min': -5,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'transport': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'waste': {
        'min': MIN_EMISSIONS,
        'max': MAX_EMISSIONS,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    # Per capita values.
    'agriculture_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'aviation_and_shipping_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'buildings_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'electricity_and_heat_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'energy_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'fugitive_emissions_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'industry_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'lucf_per_capita': {
        'min': -5,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'manufacturing_and_construction_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'other_fuel_combustion_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'total_excluding_lucf_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'total_including_lucf_per_capita': {
        'min': -5,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'transport_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
    'waste_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS,
    },
}

ERROR_METRIC = {
    'name': 'mape',
    'function': sanity_checks.mean_absolute_percentage_error,
    'min_relevant': 20,
}


def load_population(name_population=NAME_POPULATION, name=NAME):
    """Load population dataset and return it with the default entity naming.

    Parameters
    ----------
    name_population : dict
        Dictionary of entity names in the population dataset.
    name : dict
        Dictionary of default entity names.

    Returns
    -------
    population_renamed : pd.DataFrame
        Population dataset.

    """
    ####################################################################################################################
    # TODO: Remove this temporary solution once OWID population is complete.
    population_dataset_url = "https://github.com/owid/owid-datasets/raw/master/datasets/Population%20(Gapminder%2C%20" \
                             "HYDE%20%26%20UN)/Population%20(Gapminder%2C%20HYDE%20%26%20UN).csv"
    population = pd.read_csv(population_dataset_url)
    ####################################################################################################################

    # TODO: Uncomment once OWID population is complete.
    # population = catalog.find("population", namespace="owid").load().reset_index()
    population_renamed = sanity_checks.rename_columns(
        data=population,
        entities_to_rename_in_columns=["country", "year"],
        name_dataset=name_population,
        name_default=name,
    )

    return population_renamed


def execute_all_checks(old, new, population):
    checks_on_single_dataset = sanity_checks.SanityChecksOnSingleDataset(
        data=new,
        name=NAME,
        variable_ranges=RANGES,
        population=population,
        min_year_latest_possible=MIN_YEAR_LATEST_POSSIBLE,
        max_year_maximum_delay=MAX_YEAR_MAXIMUM_DELAY)

    checks_comparing_datasets = sanity_checks.SanityChecksComparingTwoDatasets(
        data_old=old,
        data_new=new,
        name=NAME,
        variable_ranges=RANGES,
        data_label_old=DATA_LABEL_OLD,
        data_label_new=DATA_LABEL_NEW,
        error_metric=ERROR_METRIC
    )

    print("Execute sanity checks on single dataset.")
    warnings_single_dataset = checks_on_single_dataset.apply_all_checks()
    summary = checks_on_single_dataset.summarize_warnings_in_html(all_warnings=warnings_single_dataset)

    print("Execute sanity checks comparing old and new datasets.")
    warnings_comparing_datasets = checks_comparing_datasets.apply_all_checks()
    summary += checks_comparing_datasets.summarize_warnings_in_html(all_warnings=warnings_comparing_datasets)
    # Add graphs to be visually inspected.
    summary += checks_comparing_datasets.summarize_figures_to_inspect_in_html(warnings=warnings_comparing_datasets)
    
    # Combine all warnings (for debugging purposes).
    all_warnings = pd.concat([warnings_single_dataset, warnings_comparing_datasets], ignore_index=True)
    
    return summary, all_warnings


def main(dataset_name, output_file):
    """Apply all sanity checks and store the result as an HTML file to be visually inspected.

    Parameters
    ----------
    data_file_old : str
        Path to old dataset file.
    data_file_new : str
        Path to new dataset file.
    output_file : str
        Path to output HTML file to be visually inspected.

    """
    print("Loading data.")
    old_raw = pd.read_csv(DATASET_OLD_FILES[dataset_name])
    new_raw = pd.read_csv(DATASET_NEW_FILES[dataset_name])

    # Prepare old and new datasets.
    old = sanity_checks.rename_columns(data=old_raw, entities_to_rename_in_columns=list(NAME_OLD[dataset_name]),
                                       name_dataset=NAME_OLD[dataset_name], name_default=NAME)
    new = sanity_checks.rename_columns(data=new_raw, entities_to_rename_in_columns=list(NAME_NEW[dataset_name]),
                                       name_dataset=NAME_NEW[dataset_name], name_default=NAME)

    print("Load population dataset.")
    population = load_population()

    # Execute all sanity checks.
    summary, all_warnings = execute_all_checks(old=old, new=new, population=population)

    print(f"Saving summary to file {output_file}.")
    with open(output_file, "w") as output_file_:
        output_file_.write(summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Perform sanity checks on the CAIT datasets, and compare the most recent "
        "version of the dataset with the previous."
    )
    parser.add_argument(
        "-n",
        "--dataset_name",
        help=f"Dataset name. Options: {', '.join(list(DATASET_NEW_FILES))}",
    )
    parser.add_argument(
        "-f",
        "--output_file",
        default=OUTPUT_FILE,
        help=f"Path to output HTML file to be visually inspected. Default: "
        f"{OUTPUT_FILE}",
    )
    parser.add_argument(
        "-s",
        "--show_in_browser",
        default=False,
        action="store_true",
        help="If given, display output file in browser.",
    )
    args = parser.parse_args()

    main(dataset_name=args.dataset_name, output_file=args.output_file)
    if args.show_in_browser:
        webbrowser.open("file://" + args.output_file)
