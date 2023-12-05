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


def df_to_json(
    complete_dataset: pd.DataFrame, output_path: str, static_columns: List[str]
) -> None:
    megajson = {}

    for _, row in tqdm(complete_dataset.iterrows(), total=len(complete_dataset)):
        row_country = row["country"]
        row_dict_static = row.drop("country")[static_columns].dropna().to_dict()
        row_dict_dynamic = row.drop("country").drop(static_columns).dropna().to_dict()

        if row_country not in megajson:
            megajson[row_country] = row_dict_static
            megajson[row_country]["data"] = [row_dict_dynamic]
        else:
            megajson[row_country]["data"].append(row_dict_dynamic)

    with open(output_path, "w") as file:
        file.write(json.dumps(megajson, indent=4))


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


def main() -> None:
    #
    # Load data.
    #
    try:
        # First try to load the latest OWID CO2 dataset from the local catalog, if it exists.
        # NOTE: If the latest dataset exists but is not found, run "reindex" from the etl poetry shell.
        tables = (
            LocalCatalog("../etl/data/", channels=["garden"])
            .find("owid_co2", namespace="emissions")
            .sort_values("version", ascending=False)
        )
    except ValueError:
        # Load the latest OWID-CO2 dataset from the remote catalog.
        tables = find(
            "owid_co2", namespace="emissions", channels=["garden"]
        ).sort_values("version", ascending=False)
    print(f"Loading owid_co2 version {tables.iloc[0].version}.")
    tb = tables.iloc[0].load()

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
    # Save dataset to files in different formats.
    pd.DataFrame(tb).to_csv(OUTPUT_CSV_FILE, index=False, float_format="%.3f")
    pd.DataFrame(tb).to_excel(OUTPUT_EXCEL_FILE, index=False, float_format="%.3f")
    df_to_json(pd.DataFrame(tb), OUTPUT_JSON_FILE, ["iso_code"])

    # Save codebook file.
    codebook.to_csv(CODEBOOK_FILE, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
