"""Constructs the `owid-co2-data.{filetype}` dataset files.

Usage:

    >>> python -m scripts.main

"""

import json
import os
import re
from functools import reduce
from typing import List, Tuple, Dict, Optional

import pandas as pd
from tqdm.auto import tqdm

from scripts import INPUT_DIR, OUTPUT_DIR
from scripts.utils import get_owid_variable
from scripts.input.variable_ids import ch4_emissions_ids, co2_emissions_ids, gdp_ids, n2o_emissions_ids,\
    population_ids, primary_energy_consumption_ids, total_ghg_emissions_ids


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
        co2_emissions_ids['Annual CO2 emissions'],
        co2_emissions_ids['Annual CO2 emissions (per capita)'],
        co2_emissions_ids['Annual CO2 emissions embedded in trade'],
        co2_emissions_ids['Annual CO2 emissions from cement'],
        co2_emissions_ids['Annual CO2 emissions from cement (per capita)'],
        co2_emissions_ids['Annual CO2 emissions from coal'],
        co2_emissions_ids['Annual CO2 emissions from coal (per capita)'],
        co2_emissions_ids['Annual CO2 emissions from flaring'],
        co2_emissions_ids['Annual CO2 emissions from flaring (per capita)'],
        co2_emissions_ids['Annual CO2 emissions from gas'],
        co2_emissions_ids['Annual CO2 emissions from gas (per capita)'],
        co2_emissions_ids['Annual CO2 emissions from oil'],
        co2_emissions_ids['Annual CO2 emissions from oil (per capita)'],
        co2_emissions_ids['Annual CO2 emissions from other industry'],
        co2_emissions_ids['Annual CO2 emissions from other industry (per capita)'],
        co2_emissions_ids['Annual CO2 emissions growth (%)'],
        co2_emissions_ids['Annual CO2 emissions growth (abs)'],
        co2_emissions_ids['Annual CO2 emissions per GDP (kg per $PPP)'],
        co2_emissions_ids['Annual CO2 emissions per unit energy (kg per kilowatt-hour)'],
        co2_emissions_ids['Annual consumption-based CO2 emissions'],
        co2_emissions_ids['Annual consumption-based CO2 emissions (per capita)'],
        co2_emissions_ids['Annual consumption-based CO2 emissions per GDP (kg per $PPP)'],
        co2_emissions_ids['Cumulative CO2 emissions'],
        co2_emissions_ids['Cumulative CO2 emissions from cement'],
        co2_emissions_ids['Cumulative CO2 emissions from coal'],
        co2_emissions_ids['Cumulative CO2 emissions from flaring'],
        co2_emissions_ids['Cumulative CO2 emissions from gas'],
        co2_emissions_ids['Cumulative CO2 emissions from oil'],
        co2_emissions_ids['Cumulative CO2 emissions from other industry'],
        co2_emissions_ids['Share of annual CO2 emissions embedded in trade'],
        co2_emissions_ids['Share of global annual CO2 emissions'],
        co2_emissions_ids['Share of global annual CO2 emissions from cement'],
        co2_emissions_ids['Share of global annual CO2 emissions from coal'],
        co2_emissions_ids['Share of global annual CO2 emissions from flaring'],
        co2_emissions_ids['Share of global annual CO2 emissions from gas'],
        co2_emissions_ids['Share of global annual CO2 emissions from oil'],
        co2_emissions_ids['Share of global annual CO2 emissions from other industry'],
        co2_emissions_ids['Share of global cumulative CO2 emissions'],
        co2_emissions_ids['Share of global cumulative CO2 emissions from cement'],
        co2_emissions_ids['Share of global cumulative CO2 emissions from coal'],
        co2_emissions_ids['Share of global cumulative CO2 emissions from flaring'],
        co2_emissions_ids['Share of global cumulative CO2 emissions from gas'],
        co2_emissions_ids['Share of global cumulative CO2 emissions from oil'],
        co2_emissions_ids['Share of global cumulative CO2 emissions from other industry'],
    ]
    dataframes = []
    codebook = []
    for var_id in tqdm(variables):
        df, meta = get_owid_variable(var_id, to_frame=True)
        assert re.search(
            r"co2", meta["name"], re.I
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
            f"Encountered an unexpected unit value for variable {var_id}: "
            f'"{meta["unit"]}". get_co2_emissions() may not work as '
            "expected."
        )
        if meta["unit"] == "tonnes":
            assert "conversionFactor" not in meta["display"], (
                f"variable {var_id} has a non-null conversion factor "
                f"({meta['display']['conversionFactor']}). Variable may not "
                "actually be stored in tonnes."
            )
            df["value"] /= 1e6  # convert tonnes to million tonnes
            new_description = re.sub("tonnes", "million tonnes", description)
            assert (
                new_description != description and "million tonnes" in new_description
            ), 'Expected "million tonnes" to be present in modified description.'
            description = new_description
        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
        codebook.append(
            {
                "name": meta["name"],
                "description": description,
                "source": meta["source"]["name"],
            }
        )
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )
    return df, codebook


def get_total_ghg_emissions() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving total GHG emissions data...")
    variables = [
        total_ghg_emissions_ids['Total including LUCF'],
        total_ghg_emissions_ids['Total including LUCF (per capita)'],
        total_ghg_emissions_ids['Total excluding LUCF'],
        total_ghg_emissions_ids['Total excluding LUCF (per capita)'],
    ]
    dataframes = []
    codebook = []
    for var_id in variables:
        df, meta = get_owid_variable(var_id, to_frame=True)
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

        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
        codebook.append(
            {
                "name": meta["name"],
                "description": description,
                "source": meta["source"]["name"],
            }
        )
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )

    # fix: replaces "European Union (27)" with "EU-27" for consistency with CO2
    # emissions dataset
    df["Country"].replace("European Union (27)", "EU-27", inplace=True)
    return df, codebook


def get_ch4_emissions() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving CH4 emissions data...")
    variables = [
        ch4_emissions_ids['Total including LUCF'],
        ch4_emissions_ids['Total including LUCF (per capita)'],
    ]
    dataframes = []
    codebook = []
    for var_id in variables:
        df, meta = get_owid_variable(var_id, to_frame=True)
        if not re.search(r"ch4", meta["name"], re.I):
            meta["name"] += " (CH4)"
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

        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
        codebook.append(
            {
                "name": meta["name"],
                "description": description,
                "source": meta["source"]["name"],
            }
        )
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )

    # fix: replaces "European Union (27)" with "EU-27" for consistency with CO2
    # emissions dataset
    df["Country"].replace("European Union (27)", "EU-27", inplace=True)
    return df, codebook


def get_n2o_emissions() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving N2O emissions data...")
    variables = [
        n2o_emissions_ids['Total including LUCF'],
        n2o_emissions_ids['Total including LUCF (per capita)'],
    ]
    dataframes = []
    codebook = []
    for var_id in variables:
        df, meta = get_owid_variable(var_id, to_frame=True)
        # appends ' (n2o)' to variable name if it does not exist, because
        # variable name for 142848 ("Total including LUCF (per capita)") does
        # not already contain N2O (for disambiguation from other gases).
        if not re.search(r"n2o", meta["name"], re.I):
            meta["name"] += " (N2O)"
        assert re.search(
            r"n2o", meta["name"], re.I
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

        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
        codebook.append(
            {
                "name": meta["name"],
                "description": description,
                "source": meta["source"]["name"],
            }
        )
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )

    # fix: replaces "European Union (27)" with "EU-27" for consistency with CO2
    # emissions dataset
    df["Country"].replace("European Union (27)", "EU-27", inplace=True)
    return df, codebook


def get_population() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving population data...")
    df, meta = get_owid_variable(
        population_ids['Population (historical estimates)'],
        to_frame=True)
    df = df.rename(
        columns={"entity": "Country", "year": "Year", "value": meta["name"]}
    ).drop(columns=["variable"])
    codebook = [
        {
            "name": meta["name"],
            "description": meta["description"],
            "source": meta["source"]["name"],
        }
    ]
    return df, codebook


def get_gdp() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving gdp data...")
    df, meta = get_owid_variable(
        gdp_ids['GDP'],
        to_frame=True)
    df = df.rename(
        columns={"entity": "Country", "year": "Year", "value": meta["name"]}
    ).drop(columns=["variable"])
    codebook = [
        {
            "name": meta["name"],
            "description": meta["description"],
            "source": meta["source"]["name"],
        }
    ]
    return df, codebook


def get_primary_energy_consumption() -> Tuple[pd.DataFrame, List[dict]]:
    print("retrieving primary energy consumption data...")
    variables = [
        primary_energy_consumption_ids['Primary energy consumption (TWh)'],
        primary_energy_consumption_ids['Energy per capita (kWh)'],
        primary_energy_consumption_ids['Energy per GDP (kWh per $)'],
    ]
    dataframes = []
    codebook = []
    for var_id in variables:
        df, meta = get_owid_variable(var_id, to_frame=True)
        assert re.search(
            r"energy", meta["name"], re.I
        ), "'energy' does not appear in energy consumption variable"
        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
        codebook.append(
            {
                "name": meta["name"],
                "description": meta["description"],
                "source": meta["source"]["name"],
            }
        )
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
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
