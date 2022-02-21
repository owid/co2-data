"""Download data from CAIT using Climate Watch Data API, process data, and export a csv file.

See https://www.climatewatchdata.org/data-explorer/historical-emissions

"""

import argparse
import os
import webbrowser
from datetime import datetime

import pandas as pd

import sanity_checks

# Define paths.
CURRENT_DIR = os.path.dirname(__file__)
DATASET_DIR = os.path.join(CURRENT_DIR, "grapher")

# Date tag and output file for visual inspection of potential issues with the dataset.
# In the output file, <DATASET_NAME> will be replaced by the name of the dataset, e.g. 'emissions_co2'.
DATE_TAG = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(CURRENT_DIR, "sanity_checks", f"cait_datasets_sanity_checks_<DATASET_NAME>_{DATE_TAG}.html")

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

# Define paths for each of the output csv files.
DATASET_NEW_FILES = {
    'emissions_all_ghg': os.path.join(DATASET_DIR, "GHG Emissions by Country and Sector (CAIT, 2021).csv"),
    'emissions_co2': os.path.join(DATASET_DIR, "CO2 emissions by sector (CAIT, 2021).csv"),
    'emissions_ch4': os.path.join(DATASET_DIR, "Methane emissions by sector (CAIT, 2021).csv"),
    'emissions_n2o': os.path.join(DATASET_DIR, "Nitrous oxide emissions by sector (CAIT, 2021).csv"),
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
    'emissions_n2o': OWID_DATASETS_URL + "Nitrous%20oxide%20emissions%20by%20sector%20(CAIT%2C%202020)/Nitrous%20oxide"
                                         "%20emissions%20by%20sector%20(CAIT%2C%202020).csv",
}

# Define entity names in the old dataset of all GHG emissions.
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

# Define entity names in the old dataset of CO2 emissions.
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

# Define entity names in the old dataset of CH4 emissions.
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

# Define entity names in the old dataset of N2O emissions.
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

# Define entity names in the new dataset of all GHG emissions.
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

# Define entity names in the new dataset of CO2 emissions.
NAME_CO2_NEW = {col: NAME_ALL_NEW[col] for col in NAME_ALL_NEW if col not in [
    'Agriculture', 'Agriculture (per capita)', 'Waste', 'Waste (per capita)']}

# Define entity names in the new dataset of CH4 emissions.
NAME_CH4_NEW = {col: NAME_ALL_NEW[col] for col in NAME_ALL_NEW if col not in [
    'Buildings', 'Buildings (per capita)', 'Electricity and heat', 'Electricity and heat (per capita)',
    'International aviation and shipping', 'International aviation and shipping (per capita)',
    'Manufacturing and construction', 'Manufacturing and construction (per capita)', 'Transport',
    'Transport (per capita)']}

# Define entity names in the new dataset of N2O emissions.
NAME_N2O_NEW = {col: NAME_ALL_NEW[col] for col in NAME_ALL_NEW if col not in [
    'Buildings', 'Buildings (per capita)', 'Electricity and heat', 'Electricity and heat (per capita)',
    'International aviation and shipping', 'International aviation and shipping (per capita)',
    'Manufacturing and construction', 'Manufacturing and construction (per capita)', 'Transport',
    'Transport (per capita)']}

# Define default entity names.
NAME = NAME_ALL_NEW.copy()

# For convenience, put all dictionaries of entity names together keyed by dataset name.
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

# Define error metric. This dictionary should contain:
# * 'name': Name of the error metric.
# * 'function': Function that ingests and old array and a new array, and returns an array of errors.
# * 'min_relevant': Minimum error to consider for warnings. Any error below this value will be ignored.
ERROR_METRIC = {
    'name': 'mape',
    'function': sanity_checks.mean_absolute_percentage_error,
    'min_relevant': 20,
}

# Latest acceptable value for the minimum year.
MIN_YEAR_LATEST_POSSIBLE = 1990
# Maximum delay (from current year) that maximum year can have.
MAX_YEAR_MAXIMUM_DELAY = 4
# Minimum acceptable value for emissions (in megatons of CO2-equivalents per year).
MIN_EMISSIONS = 0
# Maximum acceptable value for emissions (in megatons of CO2-equivalents per year).
# If equal to 'World', the world's maximum will be used.
MAX_EMISSIONS = 'World'
# Minimum acceptable value for emissions per capita (in tons of CO2-equivalents per year).
MIN_EMISSIONS_PER_CAPITA = 0
# Maximum acceptable value for emissions (in tons of CO2-equivalents per year).
MAX_EMISSIONS_PER_CAPITA = 100
# Minimum emissions to consider relevant when calculating deviations (in megatons of CO2-equivalents per year).
# This should be a value such that, if neither old nor new datasets surpass it (in absolute value), no deviation will be
# calculated. We do so to avoid having large errors on small values.
MIN_RELEVANT_EMISSIONS = 10
# Idem for per-capita emissions (in tons of CO2-equivalents per year).
MIN_RELEVANT_EMISSIONS_PER_CAPITA = 1
# Ranges of values of all variables. Each key in this dictionary corresponds to a variable (defined using
# default entity names), and the value is another dictionary, containing at least the following keys:
# * 'min': Minimum value allowed for variable.
# * 'max': Maximum value allowed for variable. As a special case, the value for 'max' can be 'World', in which
#   case we assume that the maximum value allowed for variable is the maximum value for the world.
# * 'min_relevant': Minimum relevant value to consider when looking for abrupt deviations. If a variable has
#   an *absolute value* smaller than min_relevant, both in old and new datasets, the deviation will not be
#   calculated for that point. This is so to avoid inspecting large errors on small, irrelevant values.
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
        'min': -800,
        'max': MAX_EMISSIONS,
        'min_relevant': 30,
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
        'min_relevant': 30,
    },
    'total_including_lucf': {
        'min': -10,
        'max': MAX_EMISSIONS,
        'min_relevant': 30,
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
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'aviation_and_shipping_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'buildings_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'electricity_and_heat_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'energy_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'fugitive_emissions_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'industry_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'lucf_per_capita': {
        'min': -15,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': 10,
    },
    'manufacturing_and_construction_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'other_fuel_combustion_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'total_excluding_lucf_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': 10,
    },
    'total_including_lucf_per_capita': {
        'min': -15,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': 10,
    },
    'transport_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
    'waste_per_capita': {
        'min': MIN_EMISSIONS_PER_CAPITA,
        'max': MAX_EMISSIONS_PER_CAPITA,
        'min_relevant': MIN_RELEVANT_EMISSIONS_PER_CAPITA,
    },
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


def check_that_energy_is_well_calculated(data, min_relevant_value=1, min_relevant_error=1, name=NAME,
                                         error_name=ERROR_METRIC['name'],
                                         error_function=ERROR_METRIC['function']):
    """Check that the column representing combined emissions in energy is the sum of the emissions from the
    corresponding columns.

    Parameters
    ----------
    data : pd.DataFrame
        Data.
    min_relevant_value : float
        Minimum relevant value to consider when looking for abrupt deviations.
    min_relevant_error : float
        Minimum error to consider for warnings. Any error below this value will be ignored.
    name : dict
        Default entity names.
    error_name : str
        Name for the error metric.
    error_function : function
        Error function, that must ingest an 'old' and 'new' arguments.

    Returns
    -------
    warnings : pd.DataFrame
        All potentially problematic data points found during check.

    """
    relevant_columns = [name['buildings'], name['electricity_and_heat'], name['fugitive_emissions'],
                        name['manufacturing_and_construction'], name['other_fuel_combustion'], name['transport']]
    energy_estimated = data[[col for col in relevant_columns if col in data.columns]].sum(axis=1)
    comparison = pd.DataFrame({'estimated': energy_estimated, 'reported': data[name['energy']]})
    select_relevant = (comparison['estimated'] > min_relevant_value) | (comparison['reported'] > min_relevant_value)
    comparison[error_name] = error_function(
        old=comparison[select_relevant]['reported'], new=comparison[select_relevant]['estimated'])
    warnings = comparison[comparison[error_name] > min_relevant_error].reset_index(drop=True)
    warnings['check_name'] = 'check_that_energy_is_well_calculated'

    return warnings


def _save_output_file(summary, output_file, dataset_name):
    output_file_parsed = output_file.replace('<DATASET_NAME>', dataset_name)
    # Ensure output folder exists.
    output_dir = os.path.abspath(os.path.dirname(output_file_parsed))
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    print(f"Saving summary to file {output_file_parsed}.")
    with open(output_file_parsed, "w") as output_file_:
        output_file_.write(summary)


def main(dataset_name, output_file, dataset_old_files=DATASET_OLD_FILES, dataset_new_files=DATASET_NEW_FILES, name=NAME,
         name_old=NAME_OLD, name_new=NAME_NEW, ranges=RANGES, min_year_latest_possible=MIN_YEAR_LATEST_POSSIBLE,
         max_year_maximum_delay=MAX_YEAR_MAXIMUM_DELAY, data_label_old=DATA_LABEL_OLD, data_label_new=DATA_LABEL_NEW,
         error_metric=ERROR_METRIC, ignore_lucf=False):
    """Apply all sanity checks and store the result as an HTML file to be visually inspected.

    Parameters
    ----------
    dataset_name : str
        Name of dataset to consider.
    output_file : str
        Path to output HTML file to be visually inspected.
    dataset_old_files : dict
        Paths (or URL) to each of the old dataset csv files.
    dataset_new_files : dict
        Paths for each of the output csv files.
    name : dict
        Dictionary of default entity names.
    name_old : dict
        Dictionary containing, for each one of the old datasets, its corresponding dictionary of entity names.
    name_new : dict
        Dictionary containing, for each one of the new datasets, its corresponding dictionary of entity names.
    ranges : dict
        Ranges of values of all variables. Each key in this dictionary corresponds to a variable (defined using
        default entity names), and the value is another dictionary, containing at least the following keys:
        * 'min': Minimum value allowed for variable.
        * 'max': Maximum value allowed for variable. As a special case, the value for 'max' can be 'World', in which
          case we assume that the maximum value allowed for variable is the maximum value for the world.
        * 'min_relevant': Minimum relevant value to consider when looking for abrupt deviations. If a variable has
          an *absolute value* smaller than min_relevant, both in old and new datasets, the deviation will not be
          calculated for that point. This is so to avoid inspecting large errors on small, irrelevant values.
    min_year_latest_possible : int
        Latest acceptable value for the minimum year.
    max_year_maximum_delay : int
        Maximum delay (from current year) that maximum year can have
    data_label_old : str
        Label to use to identify old dataset (in plots).
    data_label_new : str
        Label to use to identify new dataset (in plots).
    error_metric : dict
        Error metric. This dictionary should contain:
        * 'name': Name of the error metric.
        * 'function': Function that ingests and old array and a new array, and returns an array of errors.
        * 'min_relevant': Minimum error to consider for warnings. Any error below this value will be ignored.
    ignore_lucf : bool
        True to ignore variables related to LUCF when comparing old and new datasets. We do this because this variable
        is particularly inconsistent among datasets (and unstable within a dataset).

    """
    print("Loading data.")
    old_raw = pd.read_csv(dataset_old_files[dataset_name])
    new_raw = pd.read_csv(dataset_new_files[dataset_name])

    # Prepare old and new datasets.
    old = sanity_checks.rename_columns(data=old_raw, entities_to_rename_in_columns=list(name_old[dataset_name]),
                                       name_dataset=name_old[dataset_name], name_default=name)
    new = sanity_checks.rename_columns(data=new_raw, entities_to_rename_in_columns=list(name_new[dataset_name]),
                                       name_dataset=name_new[dataset_name], name_default=name)

    print("Load population dataset.")
    population = load_population()

    # Execute all sanity checks.
    print("Execute sanity checks on the new dataset.")
    checks_on_single_dataset = sanity_checks.SanityChecksOnSingleDataset(
        data=new,
        name=name,
        variable_ranges=ranges,
        population=population,
        min_year_latest_possible=min_year_latest_possible,
        max_year_maximum_delay=max_year_maximum_delay,
    )
    # Add custom checks on the new dataset.
    checks_on_single_dataset.check_that_energy_is_well_calculated =\
        lambda: check_that_energy_is_well_calculated(data=new)
    # Gather all warnings from checks.
    warnings_single_dataset = checks_on_single_dataset.apply_all_checks()
    summary = checks_on_single_dataset.summarize_warnings_in_html(all_warnings=warnings_single_dataset)

    print("Execute sanity checks comparing old and new datasets.")
    checks_comparing_datasets = sanity_checks.SanityChecksComparingTwoDatasets(
        data_old=old,
        data_new=new,
        name=name,
        variable_ranges=ranges,
        data_label_old=data_label_old,
        data_label_new=data_label_new,
        error_metric=error_metric,
    )
    warnings_comparing_datasets = checks_comparing_datasets.apply_all_checks()
    # Since data related to LUCF is highly inconsistent (for many countries), optionally ignore these variables to avoid
    # an overwhelming amount of warnings.
    if ignore_lucf:
        allowed_variables = [name[col] for col in name if 'lucf' not in col]
        warnings_comparing_datasets = warnings_comparing_datasets[
            (warnings_comparing_datasets[
                 'check_name'] == 'check_that_values_did_not_change_abruptly_from_old_to_new_dataset') &
            (warnings_comparing_datasets['Variable'].isin(allowed_variables))
        ]
    summary += checks_comparing_datasets.summarize_warnings_in_html(all_warnings=warnings_comparing_datasets)
    # Add graphs to be visually inspected.
    summary += checks_comparing_datasets.summarize_figures_to_inspect_in_html(warnings=warnings_comparing_datasets)

    _save_output_file(summary, output_file, dataset_name)


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
        help=f"Path to output HTML file to be visually inspected. Default: {OUTPUT_FILE} (where <DATASET_NAME> will be "
             f"replaced by the chosen dataset name)",
    )
    parser.add_argument(
        "-s",
        "--show_in_browser",
        default=False,
        action="store_true",
        help="If given, display output file in browser.",
    )
    parser.add_argument(
        "-i",
        "--ignore_lucf",
        default=False,
        action="store_true",
        help="If given, ignore variables related to LUCF when checking for abrupt changes.",
    )
    args = parser.parse_args()

    main(dataset_name=args.dataset_name, output_file=args.output_file, ignore_lucf=args.ignore_lucf)
    if args.show_in_browser:
        webbrowser.open("file://" + os.path.abspath(args.output_file))

# Conclusions:
# * All variables related to LUCF are significantly inconsistent between old and new datasets (for many countries).
# * There are several countries whose data has changed abruptly. We disregard those cases where (after visual
#   inspection) the new data is more stable than the old one.
# * Industry in France and Italy went up abruptly and systematically for all years.
