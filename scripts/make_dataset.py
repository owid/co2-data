"""Generate OWID CO2 dataset from most up-to-date sources.

Running this script will generate the full energy dataset in three different formats:
* owid-co2-data.csv
* owid-co2-data.xlsx
* owid-co2-data.json

"""

import argparse
import json
from typing import List
import pandas as pd
from owid import catalog
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

    for _, row in complete_dataset.iterrows():
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


def prepare_data(table: catalog.Table) -> pd.DataFrame:
    # Create a dataframe with a dummy index from the table.
    df = pd.DataFrame(table).reset_index()

    # Sort rows and columns conveniently.
    df = df.sort_values(["country", "year"]).reset_index(drop=True)
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    df = df[
        first_columns
        + [column for column in sorted(df.columns) if column not in first_columns]
    ]

    return df


def prepare_codebook(table: catalog.Table) -> pd.DataFrame:
    table = table.reset_index()

    # Manually edit some of the metadata fields.
    table["country"].metadata.description = "Geographic location."
    table["country"].metadata.sources = [catalog.meta.Source(name="Our World in Data")]
    table["year"].metadata.description = "Year of observation."
    table["year"].metadata.sources = [catalog.meta.Source(name="Our World in Data")]
    table[
        "iso_code"
    ].metadata.description = "ISO 3166-1 alpha-3, three-letter country codes."
    table["iso_code"].metadata.sources = [
        catalog.meta.Source(name="International Organization for Standardization")
    ]
    table["population"].metadata.description = "Population of each country or region."
    table["population"].metadata.sources = [
        catalog.meta.Source(
            name="Our World in Data based on different sources (https://ourworldindata.org/population-sources)"
        )
    ]

    # Gather column names, descriptions and sources from the variables' metadata.
    metadata = {"column": [], "description": [], "source": []}
    for column in table.columns:
        metadata["column"].append(column)
        metadata["description"].append(table[column].metadata.description)
        metadata["source"].append(table[column].metadata.sources[0].name)

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
    # Load latest OWID-CO2 dataset from the catalog.
    table = (
        catalog.find("owid_co2", namespace="emissions", channels=["garden"])
        .sort_values("version", ascending=False)
        .iloc[0]
        .load()
    )

    #
    # Process data.
    #
    # Minimum processing of the data.
    df = prepare_data(table=table)

    # Prepare codebook.
    codebook = prepare_codebook(table=table)

    # Sanity check.
    error = "Codebook column descriptions are not in the same order as data columns."
    assert codebook["column"].tolist() == df.columns.tolist(), error

    #
    # Save outputs.
    #
    # Save dataset to files in different formats.
    df.to_csv(OUTPUT_CSV_FILE, index=False, float_format="%.3f")
    df.to_excel(OUTPUT_EXCEL_FILE, index=False, float_format="%.3f")
    df_to_json(df, OUTPUT_JSON_FILE, ["iso_code"])

    # Save codebook file.
    codebook.to_csv(CODEBOOK_FILE, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
