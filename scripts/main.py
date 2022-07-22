"""Constructs the `owid-co2-data.{filetype}` dataset files.

Usage:

    >>> python -m scripts.main

"""

import json
import os
import re
from typing import List, Tuple, Dict, Optional, Any

import pandas as pd
import requests

from scripts import INPUT_DIR, OUTPUT_DIR

from utils import API_URL, fetch_data_and_metadata_from_api


def main():
    df, codebook = load_and_merge_datasets()
    df.pipe(rename_columns).pipe(subset_columns).pipe(reorder_columns).pipe(export)
    codebook.to_csv(os.path.join(OUTPUT_DIR, "owid-co2-codebook.csv"), index=False)


def load_and_merge_datasets() -> Tuple[pd.DataFrame, pd.DataFrame]:
    codebook_index = get_index_codebook()
    df_co2, codebook_co2 = get_co2_emissions()
    df_ghg, codebook_ghg = get_total_ghg_emissions()
    df_ch4, codebook_ch4 = get_ch4_emissions()
    df_n2o, codebook_n2o = get_n2o_emissions()
    df_pop, codebook_pop = get_population()
    df_gdp, codebook_gdp = get_gdp()
    df_energy, codebook_energy = get_primary_energy_consumption()

    # constructs the codebook
    codebook = pd.DataFrame(
        codebook_index
        + codebook_co2
        + codebook_ghg
        + codebook_ch4
        + codebook_n2o
        + codebook_pop
        + codebook_gdp
        + codebook_energy
    )
    owid_name2column_name = load_owid_name2column_name()
    codebook["column"] = codebook["name"].apply(lambda x: owid_name2column_name[x])
    codebook.drop(columns=["name"], inplace=True)
    codebook = codebook[["column", "description", "source"]]
    assert codebook.notnull().all().all(), "One or more codebook cells are NaN."

    # merges together the emissions datasets
    df = (
        pd.merge(df_co2, df_ghg, on=["Year", "Country"], how="outer", validate="1:1")
        .merge(df_ch4, on=["Year", "Country"], how="outer", validate="1:1")
        .merge(df_n2o, on=["Year", "Country"], how="outer", validate="1:1")
    )

    # sanity check: all country-year rows should contain at least one non-NaN value.
    row_has_data = df.drop(columns=["Country", "Year"]).notnull().any(axis=1)
    assert (
        row_has_data.all()
    ), "One or more country-year observations contains only NaN values."

    # merges non-emissions datasets onto emissions dataset
    df = (
        df.merge(df_energy, on=["Year", "Country"], how="left", validate="1:1")
        .merge(df_pop, on=["Year", "Country"], how="left", validate="1:1")
        .merge(df_gdp, on=["Year", "Country"], how="left", validate="1:1")
    )

    df["ISO code"] = get_iso_codes(df["Country"])
    df = df.round(3)
    df.sort_values(["Country", "Year"], inplace=True)

    return df, codebook


def get_co2_emissions() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving CO2 emissions data...")

    variables = [
        "Annual CO2 emissions",
        "Annual CO2 emissions (per capita)",
        "Annual CO2 emissions embedded in trade",
        "Annual CO2 emissions from cement",
        "Annual CO2 emissions from cement (per capita)",
        "Annual CO2 emissions from coal",
        "Annual CO2 emissions from coal (per capita)",
        "Annual CO2 emissions from flaring",
        "Annual CO2 emissions from flaring (per capita)",
        "Annual CO2 emissions from gas",
        "Annual CO2 emissions from gas (per capita)",
        "Annual CO2 emissions from oil",
        "Annual CO2 emissions from oil (per capita)",
        "Annual CO2 emissions from other industry",
        "Annual CO2 emissions from other industry (per capita)",
        "Annual CO2 emissions growth (%)",
        "Annual CO2 emissions growth (abs)",
        "Annual CO2 emissions per GDP (kg per $PPP)",
        "Annual CO2 emissions per unit energy (kg per kilowatt-hour)",
        "Annual consumption-based CO2 emissions",
        "Annual consumption-based CO2 emissions (per capita)",
        "Annual consumption-based CO2 emissions per GDP (kg per $PPP)",
        "Cumulative CO2 emissions",
        "Cumulative CO2 emissions from cement",
        "Cumulative CO2 emissions from coal",
        "Cumulative CO2 emissions from flaring",
        "Cumulative CO2 emissions from gas",
        "Cumulative CO2 emissions from oil",
        "Cumulative CO2 emissions from other industry",
        "Share of annual CO2 emissions embedded in trade",
        "Share of global annual CO2 emissions",
        "Share of global annual CO2 emissions from cement",
        "Share of global annual CO2 emissions from coal",
        "Share of global annual CO2 emissions from flaring",
        "Share of global annual CO2 emissions from gas",
        "Share of global annual CO2 emissions from oil",
        "Share of global annual CO2 emissions from other industry",
        "Share of global cumulative CO2 emissions",
        "Share of global cumulative CO2 emissions from cement",
        "Share of global cumulative CO2 emissions from coal",
        "Share of global cumulative CO2 emissions from flaring",
        "Share of global cumulative CO2 emissions from gas",
        "Share of global cumulative CO2 emissions from oil",
        "Share of global cumulative CO2 emissions from other industry",
    ]

    df, metadata = fetch_data_and_metadata_from_api(
        "backport/owid/latest/dataset_5582_global_carbon_budget__global_carbon_project__v2021/dataset_5582_global_carbon_budget__global_carbon_project__v2021",
        variables,
    )

    codebook = []
    for var_title in variables:
        meta = [v for v in metadata["variables"] if v["title"] == var_title][0]

        assert re.search(
            r"co2", meta["title"], re.I
        ), "'co2' does not appear in co2 emissions variable"
        # converts tonnes to million tonnes for CO2 emissions variables with
        # unit=="tonnes".
        description = meta["description"]
        assert meta["unit"] in [
            "tonnes",
            "tonnes per capita",
            "%",
            "kilograms per $PPP",
            "kilograms per kilowatt-hour",
        ], (
            f"Encountered an unexpected unit value for variable {var_title}: "
            f'"{meta["unit"]}". get_co2_emissions() may not work as '
            "expected."
        )
        if meta["unit"] == "tonnes":
            assert "conversionFactor" not in meta["display"], (
                f"variable {var_title} has a non-null conversion factor "
                f"({meta['display']['conversionFactor']}). Variable may not "
                "actually be stored in tonnes."
            )
            df[var_title] /= 1e6  # convert tonnes to million tonnes
            new_description = re.sub("tonnes", "million tonnes", description)
            assert (
                new_description != description and "million tonnes" in new_description
            ), 'Expected "million tonnes" to be present in modified description.'
            description = new_description
        codebook.append(
            {
                "name": meta["title"],
                "description": meta["description"],
                "source": meta["sources"][0]["name"],
            }
        )

    return df, codebook


def get_total_ghg_emissions() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving total GHG emissions data...")

    variables = [
        "Total including LUCF",
        "Total including LUCF (per capita)",
        "Total excluding LUCF",
        "Total excluding LUCF (per capita)",
    ]

    df, metadata = fetch_data_and_metadata_from_api(
        "backport/owid/latest/dataset_5524_ghg_emissions_by_country_and_sector__cait__2021/dataset_5524_ghg_emissions_by_country_and_sector__cait__2021",
        variables,
    )

    codebook = []
    for var_title in variables:
        meta = [v for v in metadata["variables"] if v["title"] == var_title][0]

        # fix: if conversionFactor==1e6, then the variable is actually stored in
        # million tonnes. Updates the description accordingly.
        description = meta["description"]
        if (
            meta["display"]["unit"] == "tonnes CO₂e"
            and meta["display"]["conversionFactor"] == 1e6
        ):
            if not re.search("million tonnes", description):
                new_description = re.sub("tonnes", "million tonnes", description)
                assert (
                    new_description != description
                    and "million tonnes" in new_description
                ), 'Expected "million tonnes" to be present in modified description.'
                description = new_description

        codebook.append(
            {
                "name": meta["title"],
                "description": description,
                "source": meta["sources"][0]["name"],
            }
        )

    # fix: replaces "European Union (27)" with "EU-27" for consistency with CO2
    # emissions dataset
    df["Country"].replace("European Union (27)", "EU-27", inplace=True)
    return df, codebook


def get_ch4_emissions() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving CH4 emissions data...")

    variables = [
        "Total including LUCF",
        "Total including LUCF (per capita)",
    ]

    df, metadata = fetch_data_and_metadata_from_api(
        "backport/owid/latest/dataset_5527_methane_emissions_by_sector__cait__2021/dataset_5527_methane_emissions_by_sector__cait__2021",
        variables,
    )

    codebook = []
    for var_title in variables:
        meta = [v for v in metadata["variables"] if v["title"] == var_title][0]

        if not re.search(r"ch4", meta["title"], re.I):
            meta["title"] += " (CH4)"
        # fix: if conversionFactor==1e6, then the variable is actually stored in
        # million tonnes. Updates the description accordingly
        description = meta["description"]
        if (
            meta["display"]["unit"] == "tonnes CO₂e"
            and meta["display"]["conversionFactor"] == 1e6
        ):
            if not re.search("million tonnes", description):
                new_description = re.sub("tonnes", "million tonnes", description)
                assert (
                    new_description != description
                    and "million tonnes" in new_description
                ), 'Expected "million tonnes" to be present in modified description.'
                description = new_description

        codebook.append(
            {
                "name": meta["title"],
                "description": description,
                "source": meta["sources"][0]["name"],
            }
        )

    # fix: replaces "European Union (27)" with "EU-27" for consistency with CO2
    # emissions dataset
    df["Country"].replace("European Union (27)", "EU-27", inplace=True)
    return df, codebook


def get_n2o_emissions() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving N2O emissions data...")
    variables = [
        "Total including LUCF",
        "Total including LUCF (per capita)",
    ]

    df, metadata = fetch_data_and_metadata_from_api(
        "backport/owid/latest/dataset_5526_nitrous_oxide_emissions_by_sector__cait__2021/dataset_5526_nitrous_oxide_emissions_by_sector__cait__2021",
        variables,
    )

    codebook = []
    for var_title in variables:
        meta = [v for v in metadata["variables"] if v["title"] == var_title][0]

        # appends ' (n2o)' to variable name if it does not exist, because
        # variable name for 142848 ("Total including LUCF (per capita)") does
        # not already contain N2O (for disambiguation from other gases).
        if not re.search(r"n2o", meta["title"], re.I):
            meta["title"] += " (N2O)"
        assert re.search(
            r"n2o", meta["title"], re.I
        ), "'n2o' does not appear in n2o emissions variable"

        # fix: if conversionFactor==1e6, then the variable is actually stored in
        # million tonnes. Updates the description accordingly
        description = meta["description"]
        if (
            meta["display"]["unit"] == "tonnes CO₂e"
            and meta["display"]["conversionFactor"] == 1e6
        ):
            if not re.search("million tonnes", description):
                new_description = re.sub("tonnes", "million tonnes", description)
                assert (
                    new_description != description
                    and "million tonnes" in new_description
                ), 'Expected "million tonnes" to be present in modified description.'
                description = new_description

        codebook.append(
            {
                "name": meta["title"],
                "description": description,
                "source": meta["sources"][0]["name"],
            }
        )

    # fix: replaces "European Union (27)" with "EU-27" for consistency with CO2
    # emissions dataset
    df["Country"].replace("European Union (27)", "EU-27", inplace=True)
    return df, codebook


def get_population() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving population data...")

    variables = ["Population (historical estimates)"]

    df, metadata = fetch_data_and_metadata_from_api(
        "backport/owid/latest/dataset_70_population__gapminder__hyde__and__un/dataset_70_population__gapminder__hyde__and__un",
        variables,
    )

    meta = [v for v in metadata["variables"] if v["title"] == variables[0]][0]

    codebook = [
        {
            "name": meta["title"],
            "description": meta["description"],
            "source": meta["sources"][0]["name"],
        }
    ]
    return df, codebook


def get_gdp() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving gdp data...")

    r = requests.get(
        f"{API_URL}/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp"
    )
    assert r.ok
    metadata = r.json()

    df = pd.read_feather(
        f"{API_URL}/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.feather"
    )
    df = df.rename(
        columns={
            "year": "Year",
            "country": "Country",
        }
    )

    # use long title instead of short names
    df = df.rename(columns={v["short_name"]: v["title"] for v in metadata["variables"]})

    df = df[["Country", "Year", "GDP"]]

    meta = [v for v in metadata["variables"] if v["title"] == "GDP"][0]

    codebook = [
        {
            "name": meta["title"],
            "description": meta["description"],
            "source": metadata["dataset"]["sources"][0]["name"],
        }
    ]
    return df, codebook


def get_primary_energy_consumption() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving primary energy consumption data...")
    variables = [
        "Primary energy consumption (TWh)",
        "Energy per capita (kWh)",
        "Energy per GDP (kWh per $)",
    ]

    df, metadata = fetch_data_and_metadata_from_api(
        "backport/owid/latest/dataset_5569_primary_energy_consumption_bp__and__eia__2022/dataset_5569_primary_energy_consumption_bp__and__eia__2022",
        variables,
    )

    codebook = []
    for var_title in variables:
        meta = [v for v in metadata["variables"] if v["title"] == var_title][0]

        assert re.search(
            r"energy", meta["title"], re.I
        ), "'energy' does not appear in energy consumption variable"
        codebook.append(
            {
                "name": meta["title"],
                "description": meta["description"],
                "source": meta["sources"][0]["name"],
            }
        )
    return df, codebook


def get_iso_codes(countries: List[str]) -> List[Optional[str]]:
    country2iso_code = (
        pd.read_csv(os.path.join(INPUT_DIR, "iso3166_1_alpha_3_codes.csv"))
        .set_index("country")
        .squeeze()
        .to_dict()
    )
    # missing_countries = sorted(set([c for c in countries if c not in country2iso_code]))
    # assert (
    #     len(missing_countries) == 0
    # ), f"The following countries do not have an ISO code: {missing_countries}"
    iso_codes = [country2iso_code.get(c) for c in countries]
    return iso_codes


def get_index_codebook() -> List[dict]:
    """constructs codebook for index columns (iso_code, country, year)."""
    codebook = [
        {
            "name": "ISO code",
            "description": "ISO 3166-1 alpha-3 – three-letter country codes",
            "source": "International Organization for Standardization",
        },
        {
            "name": "Country",
            "description": "Geographic location",
            "source": "Our World in Data",
        },
        {
            "name": "Year",
            "description": "Year of observation",
            "source": "Our World in Data",
        },
    ]
    return codebook


def load_owid_name2column_name() -> Dict[str, str]:
    """loads dictionary of OWID variable names to cleaned column names to use
    in this dataset export."""
    with open(os.path.join(INPUT_DIR, "owid_name2column_name.json"), "r") as f:
        owid_name2column_name = json.load(f)
    return owid_name2column_name


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


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        errors="raise",
        columns=load_owid_name2column_name(),
    )


def subset_columns(df: pd.DataFrame) -> pd.DataFrame:
    """drops all columns that are not in the codebook"""
    codebook = pd.read_csv(os.path.join(OUTPUT_DIR, "owid-co2-codebook.csv"))
    assert (
        codebook["column"].isin(df.columns).all()
    ), "One or more columns in the codebook are not present in the combined dataframe."
    return df[codebook["column"].tolist()]


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """reorders columns so that index columns are first."""
    return df.set_index(["iso_code", "country", "year"]).reset_index()


def export(df: pd.DataFrame) -> None:
    print("Exporting data to csv, xlsx, and json...")
    df.sort_values(["country", "year"], inplace=True)
    df.to_csv(os.path.join(OUTPUT_DIR, "owid-co2-data.csv"), index=False)
    df.to_excel(os.path.join(OUTPUT_DIR, "owid-co2-data.xlsx"), index=False)
    df_to_json(df, os.path.join(OUTPUT_DIR, "owid-co2-data.json"), ["iso_code"])


if __name__ == "__main__":
    main()
