"""Constructs the `owid-co2-data.{filetype}` dataset files.

Usage:

    >>> python -m scripts.main

"""

import os
import json
import re
import pandas as pd
from typing import List, Optional
from functools import reduce
from tqdm import tqdm

from scripts import INPUT_DIR, OUTPUT_DIR
from scripts.utils import get_owid_variable


def main():
    df = load_and_merge_datasets()
    df.pipe(rename_columns).pipe(subset_columns).pipe(reorder_columns).pipe(export)


def load_and_merge_datasets() -> pd.DataFrame:
    df_co2 = get_co2_emissions()
    df_ghg = get_total_ghg_emissions()
    df_ch4 = get_ch4_emissions()
    df_n2o = get_n2o_emissions()
    df_pop = get_population()
    df_gdp = get_gdp()
    df_energy = get_primary_energy_consumption()

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

    df["iso_code"] = get_iso_codes(df["Country"])
    df = df.round(3)
    df.sort_values(["Country", "Year"], inplace=True)

    return df


def get_co2_emissions() -> pd.DataFrame:
    print("retrieving CO2 emissions data...")
    variables = [
        176101,
        176135,
        176131,
        176132,
        176105,
        176102,
        176136,
        176153,
        176138,
        176160,
        176133,
        176137,
        176134,
        176111,
        176107,
        176115,
        176119,
        176123,
        176127,
        176108,
        176112,
        176116,
        176120,
        176124,
        176128,
        176152,
        176154,
        176155,
        176156,
        176157,
        176158,
        176159,
        176140,
        176142,
        176144,
        176146,
        176148,
        176150,
        176161,
        176162,
        176163,
        176164,
        176165,
        176166,
    ]
    dataframes = []
    for var_id in tqdm(variables):
        df, meta = get_owid_variable(var_id, to_frame=True)
        assert re.search(
            r"co2", meta["name"], re.I
        ), "'co2' does not appear in co2 emissions variable"
        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )
    return df


def get_total_ghg_emissions() -> pd.DataFrame:
    print("retrieving total GHG emissions data...")
    variables = [142731, 142823]
    dataframes = []
    for var_id in variables:
        df, meta = get_owid_variable(var_id, to_frame=True)
        assert re.search(
            r"ghg", meta["name"], re.I
        ), "'ghg' does not appear in ghg emissions variable"
        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )
    return df


def get_ch4_emissions() -> pd.DataFrame:
    print("retrieving CH4 emissions data...")
    variables = [142803, 142841]
    dataframes = []
    for var_id in variables:
        df, meta = get_owid_variable(var_id, to_frame=True)
        assert re.search(
            r"ch4", meta["name"], re.I
        ), "'ch4' does not appear in ch4 emissions variable"
        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )
    return df


def get_n2o_emissions() -> pd.DataFrame:
    print("retrieving N2O emissions data...")
    variables = [142812, 142848]
    dataframes = []
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
        df = df.rename(
            columns={
                "entity": "Country",
                "year": "Year",
                "value": meta["name"],
            }
        ).drop(columns=["variable"])
        dataframes.append(df)
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )
    return df


def get_population() -> pd.DataFrame:
    print("retrieving population data...")
    df = (
        get_owid_variable(72, to_frame=True)[0]
        .rename(columns={"entity": "Country", "year": "Year", "value": "Population"})
        .drop(columns=["variable"])
    )
    return df


def get_gdp() -> pd.DataFrame:
    print("retrieving gdp data...")
    df = (
        get_owid_variable(146201, to_frame=True)[0]
        .rename(
            columns={"entity": "Country", "year": "Year", "value": "Total real GDP"}
        )
        .drop(columns=["variable"])
    )
    return df


def get_primary_energy_consumption() -> pd.DataFrame:
    print("retrieving primary energy consumption data...")
    variables = [143360, 143363, 143364]
    dataframes = []
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
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Country", "Year"], how="outer", validate="1:1"
        ),
        dataframes,
    )
    return df


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
        columns={
            "Country": "country",
            "Year": "year",
            "Annual CO2 emissions": "co2",
            "Annual consumption-based CO2 emissions": "consumption_co2",
            "Annual CO2 emissions growth (abs)": "co2_growth_abs",
            "Annual CO2 emissions growth (%)": "co2_growth_prct",
            "Annual CO2 emissions embedded in trade": "trade_co2",
            "Share of annual CO2 emissions embedded in trade": "trade_co2_share",
            "Annual CO2 emissions (per capita)": "co2_per_capita",
            "Annual consumption-based CO2 emissions (per capita)": "consumption_co2_per_capita",
            "Share of global annual CO2 emissions": "share_global_co2",
            "Cumulative CO2 emissions": "cumulative_co2",
            "Share of global cumulative CO2 emissions": "share_global_cumulative_co2",
            "Annual CO2 emissions per GDP (kg per $PPP)": "co2_per_gdp",
            "Annual consumption-based CO2 emissions per GDP (kg per $PPP)": "consumption_co2_per_gdp",
            "Annual CO2 emissions per unit energy (kg per kilowatt-hour)": "co2_per_unit_energy",
            "Annual CO2 emissions from cement": "cement_co2",
            "Annual CO2 emissions from coal": "coal_co2",
            "Annual CO2 emissions from oil": "oil_co2",
            "Annual CO2 emissions from gas": "gas_co2",
            "Annual CO2 emissions from flaring": "flaring_co2",
            "Annual CO2 emissions from other industry": "other_industry_co2",
            "Annual CO2 emissions from cement (per capita)": "cement_co2_per_capita",
            "Annual CO2 emissions from coal (per capita)": "coal_co2_per_capita",
            "Annual CO2 emissions from oil (per capita)": "oil_co2_per_capita",
            "Annual CO2 emissions from gas (per capita)": "gas_co2_per_capita",
            "Annual CO2 emissions from flaring (per capita)": "flaring_co2_per_capita",
            "Annual CO2 emissions from other industry (per capita)": "other_co2_per_capita",
            "Share of global annual CO2 emissions from coal": "share_global_coal_co2",
            "Share of global annual CO2 emissions from oil": "share_global_oil_co2",
            "Share of global annual CO2 emissions from gas": "share_global_gas_co2",
            "Share of global annual CO2 emissions from flaring": "share_global_flaring_co2",
            "Share of global annual CO2 emissions from cement": "share_global_cement_co2",
            "Share of global annual CO2 emissions from other industry": "share_global_other_co2",
            "Cumulative CO2 emissions from coal": "cumulative_coal_co2",
            "Cumulative CO2 emissions from oil": "cumulative_oil_co2",
            "Cumulative CO2 emissions from gas": "cumulative_gas_co2",
            "Cumulative CO2 emissions from flaring": "cumulative_flaring_co2",
            "Cumulative CO2 emissions from cement": "cumulative_cement_co2",
            "Cumulative CO2 emissions from other industry": "cumulative_other_co2",
            "Share of global cumulative CO2 emissions from coal": "share_global_cumulative_coal_co2",
            "Share of global cumulative CO2 emissions from oil": "share_global_cumulative_oil_co2",
            "Share of global cumulative CO2 emissions from gas": "share_global_cumulative_gas_co2",
            "Share of global cumulative CO2 emissions from flaring": "share_global_cumulative_flaring_co2",
            "Share of global cumulative CO2 emissions from cement": "share_global_cumulative_cement_co2",
            "Share of global cumulative CO2 emissions from other industry": "share_global_cumulative_other_co2",
            "Total GHG emissions including LUCF (CAIT)": "total_ghg",
            "Total including LUCF (per capita) (GHG Emissions, CAIT)": "ghg_per_capita",
            "Total including LUCF (CH4 emissions, CAIT)": "methane",
            "Total including LUCF (per capita) (CH4 emissions, CAIT)": "methane_per_capita",
            "Total including LUCF (N2O emissions, CAIT)": "nitrous_oxide",
            "Total including LUCF (per capita) (N2O)": "nitrous_oxide_per_capita",
            "Primary energy consumption (TWh)": "primary_energy_consumption",
            "Energy consumption per capita (kWh)": "energy_per_capita",
            "Energy consumption per GDP (kWh per $)": "energy_per_gdp",
            "Population": "population",
            "Total real GDP": "gdp",
        },
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
