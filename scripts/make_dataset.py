"""Generate OWID CO2 dataset from most up-to-date sources.

Running this script will generate the full energy dataset in three different formats:
* owid-co2-data.csv
* owid-co2-data.xlsx
* owid-co2-data.json

"""

import argparse
import json
import re
from typing import List
import pandas as pd
from owid.catalog import LocalCatalog, Source, Table, find
from tqdm.auto import tqdm
from scripts import OUTPUT_DIR

# Define paths to output files.
OUTPUT_CSV_FILE = OUTPUT_DIR / "owid-co2-data.csv"
OUTPUT_EXCEL_FILE = OUTPUT_DIR / "owid-co2-data.xlsx"
OUTPUT_JSON_FILE = OUTPUT_DIR / "owid-co2-data.json"
CODEBOOK_FILE = OUTPUT_DIR / "owid-co2-codebook.csv"


def save_data_to_json(tb: Table, output_path: str) -> None:
    tb = tb.copy()

    # Initialize output dictionary, that contains one item per country in the data.
    output_dict = {}

    # Each country contains a dictionary, which contains:
    # * "iso_code", which is the ISO code (as a string), if it exists.
    # * "data", which is a list of dictionaries, one per year.
    #   Each dictionary contains "year" as the first item, followed by all other non-nan indicator values for that year.
    for country in sorted(set(tb["country"])):
        # Initialize output dictionary for current country.
        output_dict[country] = {}

        # If there is an ISO code for this country, add it as a new item of the dictionary.
        iso_code = tb[tb["country"]==country].iloc[0]["iso_code"]
        if not pd.isna(iso_code):
            output_dict[country]["iso_code"] = iso_code

        # Create the data dictionary for this country.
        dict_country = tb[tb["country"] == country].drop(columns=["country", "iso_code"]).to_dict(orient="records")
        # Remove all nans.
        data_country = [{indicator:value for indicator, value in d_year.items() if not pd.isna(value)} for d_year in dict_country]
        output_dict[country]["data"] = data_country

    # Write dictionary to file as a big json object.
    with open(output_path, "w") as file:
        file.write(json.dumps(output_dict, indent=4))


def prepare_data(tb: Table) -> Table:
    # Sort rows and columns conveniently.
    tb = tb.reset_index().sort_values(["country", "year"]).reset_index(drop=True)
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    tb = tb[
        first_columns
        + [column for column in sorted(tb.columns) if column not in first_columns]
    ]

    return tb


def remove_details_on_demand(text: str) -> str:
    # Remove references to details on demand from a text.
    # Example: "This is a [description](#dod:something)." -> "This is a description."
    regex = r"\(\#dod\:.*\)"
    if "(#dod:" in text:
        text = re.sub(regex, "", text).replace("[", "").replace("]", "")

    return text


def prepare_codebook(tb: Table) -> pd.DataFrame:
    table = tb.copy()

    # Manually edit some of the metadata fields.
    table["country"].metadata.description = "Geographic location."
    table["country"].metadata.sources = [Source(name="Our World in Data")]
    table["country"].metadata.origins = []
    table["year"].metadata.description = "Year of observation."
    table["year"].metadata.sources = [Source(name="Our World in Data")]
    table["year"].metadata.origins = []
    table[
        "iso_code"
    ].metadata.description = "ISO 3166-1 alpha-3, three-letter country codes."
    table["iso_code"].metadata.sources = [
        Source(name="International Organization for Standardization")
    ]
    table["iso_code"].metadata.origins = []

    # Gather column names, descriptions and sources from the variables' metadata.
    metadata = {"column": [], "description": [], "source": []}
    for column in table.columns:
        metadata["column"].append(column)
        # Prepare indicator's description.
        if table[column].metadata.description:
            description = table[column].metadata.description
        else:
            description = f"{table[column].metadata.title} - {table[column].metadata.description_short}"
            description = remove_details_on_demand(description)
        metadata["description"].append(description)
        # Gather unique sources of current variable.
        unique_sources = []
        for source in table[column].metadata.sources:
            source_name = source.name
            # Some source names end in a period. Remove it.
            if source_name[-1] == ".":
                source_name = source_name[:-1]

            # Remove the "Our World in Data based on" from all sources.
            source_name = source_name.replace("Our World in Data based on ", "")

            # Add url at the end of the source (if any url is given).
            if source.url:
                source_name += f" [{source.url}]"

            if source_name not in unique_sources:
                unique_sources.append(source_name)
        for origin in table[column].metadata.origins:
            # Construct the source name from the origin's attribution.
            # If not defined, build it using the default format "Producer - Data product (year)".
            source_name = (
                origin.attribution
                or f"{origin.producer} - {origin.title or origin.title_snapshot} ({origin.date_published.split('-')[0]})"
            )

            # Add url at the end of the source.
            if origin.url_main:
                source_name += f" [{origin.url_main}]"

            # Add the source to the list of unique sources.
            if source_name not in unique_sources:
                unique_sources.append(source_name)

        # Concatenate all sources.
        sources_combined = "; ".join(unique_sources)
        metadata["source"].append(sources_combined)

    # Create a dataframe with the gathered metadata and sort conveniently by column name.
    codebook = pd.DataFrame(metadata).set_index("column").sort_index()
    # For clarity, ensure column descriptions are in the same order as the columns in the data.
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    codebook = pd.concat(
        [codebook.loc[first_columns], codebook.drop(first_columns)]
    ).reset_index()

    return codebook


def load_latest_dataset(dataset_name: str = "owid_co2", namespace: str="emissions",
                        path_to_local_catalog: str = "../etl/data/", channel:str = "garden") -> Table:
    try:
        # First try to load the latest OWID energy dataset from the local catalog, if it exists.
        tables = (
            LocalCatalog(path_to_local_catalog, channels=[channel])
            .find(dataset_name, namespace=namespace)
            .sort_values("version", ascending=False)
        )
    except ValueError:
        # Load the latest OWID energy dataset from the remote catalog.
        tables = find(
            dataset_name, namespace=namespace, channels=[channel]
        ).sort_values("version", ascending=False)
    table_selected = tables.iloc[0]
    tb = table_selected.load()
    print(f"Loaded: {table_selected.path}")

    return tb


def main() -> None:
    #
    # Load data.
    #
    # Load latest dataset from etl (from a local or otherwise a remote catalog).
    # NOTE: If the latest dataset exists but is not found, run "reindex" from the etl poetry shell.
    tb = load_latest_dataset()

    #
    # Process data.
    #
    # Minimum processing of the data.
    tb = prepare_data(tb=tb)

    # Prepare codebook.
    codebook = prepare_codebook(tb=tb)

    # Sanity check.
    error = "Codebook column descriptions are not in the same order as data columns."
    assert codebook["column"].tolist() == tb.columns.tolist(), error

    #
    # Save outputs.
    #
    # Save data to a csv file.
    # NOTE: First convert to dataframe to avoid saving metadata as an additional json file.
    pd.DataFrame(tb).to_csv(OUTPUT_CSV_FILE, index=False, float_format="%.3f")
    # Save data and codebook to an excel file.
    with pd.ExcelWriter(OUTPUT_EXCEL_FILE) as writer:
        tb.to_excel(writer, sheet_name='Data', index=False, float_format="%.3f")
        codebook.to_excel(writer, sheet_name='Metadata')
    # Save data to json.
    save_data_to_json(tb, OUTPUT_JSON_FILE)
    # Save codebook file.
    codebook.to_csv(CODEBOOK_FILE, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
