import os
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(CURRENT_DIR, "input")
TMP_DIR = os.path.join(CURRENT_DIR, "tmp")
GRAPHER_DIR = os.path.join(CURRENT_DIR, "grapher")


def main():
    # GCP data
    gas_gcp = pd.read_excel(
        os.path.join(INPUT_DIR, "country_fuel/gas_by_country.xlsx"),
        sheet_name="Data", skiprows=1
    )
    oil_gcp = pd.read_excel(
        os.path.join(INPUT_DIR, "country_fuel/oil_by_country.xlsx"),
        sheet_name="Data", skiprows=1
    )
    coal_gcp = pd.read_excel(
        os.path.join(INPUT_DIR, "country_fuel/coal_by_country.xlsx"),
        sheet_name="Data", skiprows=1
    )
    flaring_gcp = pd.read_excel(
        os.path.join(INPUT_DIR, "country_fuel/flaring_by_country.xlsx"),
        sheet_name="Data", skiprows=1
    )
    cement_gcp = pd.read_excel(
        os.path.join(INPUT_DIR, "country_fuel/cement_by_country.xlsx"),
        sheet_name="Data", skiprows=1
    )
    country_gcp = pd.read_csv(
        os.path.join(INPUT_DIR, "shared/gcp_country_standardized.csv")
    )

    gas_gcp = pd.melt(gas_gcp, id_vars=["Year"], var_name=["Country"], value_name="Gas")
    oil_gcp = pd.melt(oil_gcp, id_vars=["Year"], var_name=["Country"], value_name="Oil")
    coal_gcp = pd.melt(coal_gcp, id_vars=["Year"], var_name=["Country"], value_name="Coal")
    flaring_gcp = pd.melt(flaring_gcp, id_vars=["Year"], var_name=["Country"], value_name="Flaring")
    cement_gcp = pd.melt(cement_gcp, id_vars=["Year"], var_name=["Country"], value_name="Cement")

    emissions_gcp = (
        gas_gcp
        .merge(oil_gcp, on=["Year", "Country"])
        .merge(coal_gcp, on=["Year", "Country"])
        .merge(flaring_gcp, on=["Year", "Country"])
        .merge(cement_gcp, on=["Year", "Country"])
    )

    emissions_gcp = (
        emissions_gcp
        .merge(country_gcp, on="Country")
        .drop(columns=["Country"])
        .rename(columns={"CountryStandardised": "Country"})
    )

    # CDIAC data
    emissions_cdiac = pd.read_csv(
        os.path.join(INPUT_DIR, "country_fuel/co2_fuel_country_cdiac.csv"),
        skiprows=[1, 2, 3],
        na_values=[".", " "]
    )

    emissions_cdiac = emissions_cdiac.rename(columns={
        "Nation": "Country",
        "Emissions from solid fuel consumption": "Coal",
        "Emissions from liquid fuel consumption": "Oil",
        "Emissions from gas fuel consumption": "Gas",
        "Emissions from cement production": "Cement",
        "Emissions from gas flaring": "Flaring"
    })

    emissions_cdiac = emissions_cdiac.drop(columns={
        "Total CO2 emissions from fossil-fuels and cement production (thousand metric tons of C)",
        "Per capita CO2 emissions (metric tons of carbon)",
        "Emissions from bunker fuels (not included in the totals)"
    })

    emissions_cdiac["Country"] = emissions_cdiac["Country"].str.capitalize()

    # Merge with country standardised names
    country_cdiac = pd.read_csv(os.path.join(INPUT_DIR, "shared/cdiac_country_standardized.csv"))
    emissions_cdiac = (
        emissions_cdiac
        .merge(country_cdiac, on="Country")
        .drop(columns=["Country"])
        .rename(columns={"CountryStandardised": "Country"})
        .groupby(["Country", "Year"], as_index=False)
        .sum()
    )

    #Convert from thousand tonnes of carbon to million tonnes of CO2
    emissions_cdiac[["Cement", "Coal", "Flaring", "Gas", "Oil"]] = (
        emissions_cdiac[["Cement", "Coal", "Flaring", "Gas", "Oil"]]
        .astype(float)
        .mul(3.664 / 1000)
    )

    # Combined CDIAC and GCP
    emissions_cdiac.loc[:, "Source"] = "CDIAC"
    emissions_cdiac.loc[:, "Priority"] = 0

    emissions_gcp.loc[:, "Source"] = "GCP"
    emissions_gcp.loc[:, "Priority"] = 1

    combined = pd.concat([emissions_cdiac, emissions_gcp])
    combined = combined.sort_values(["Country", "Year", "Priority"])
    combined = combined.groupby(["Year", "Country"]).tail(1)

    # Drop columns
    combined = combined.drop(columns=["Priority", "Source"])

    # Reorder columns
    other_columns = sorted([col for col in combined.columns if col not in ["Country", "Year"]])
    combined = combined[["Country", "Year"] + other_columns]

    # Merge with global figures
    world = pd.read_csv(os.path.join(TMP_DIR, "global_co2_fuel_type.csv"))
    combined = pd.concat([combined, world])

    # Import population dataset
    population = pd.read_csv(os.path.join(INPUT_DIR, "shared/population.csv"))

    # Add population
    combined = combined.merge(population, on=["Country", "Year"])

    # Calculate per capita figures
    per_capita_cols = ["Cement", "Coal", "Flaring", "Gas", "Oil"]
    for col in per_capita_cols:
        combined[f"{col} (per capita)"] = combined[col] / combined["Population"] * 1000000

    # Drop "Population" column
    combined = combined.drop(columns=["Population", "Total emissions"])

    # Save to CSV file
    combined.to_csv(
        os.path.join(GRAPHER_DIR, "co2_fuel.csv"), index=False
    )


if __name__ == '__main__':
    main()
