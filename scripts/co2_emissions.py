import os
import pandas as pd
import numpy as np


CURRENT_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(CURRENT_DIR, "input")
GRAPHER_DIR = os.path.join(CURRENT_DIR, "grapher")
GCP_CO2_CONVERSION = 3.664


def add_population(dataframe):
    population = pd.read_csv(os.path.join(INPUT_DIR, "shared/population.csv"))
    dataframe = dataframe.merge(population, on=["Country", "Year"], how="left")
    return dataframe


def add_gdp(dataframe):
    gdp = pd.read_csv(
        os.path.join(INPUT_DIR, "shared/total-gdp-maddison.csv"),
        usecols=["Entity", "Year", "Total real GDP"]
    )
    gdp = gdp.rename(columns={"Entity": "Country"})
    dataframe = dataframe.merge(gdp, on=["Country", "Year"], how="left")
    return dataframe


def add_energy(dataframe):
    energy = pd.read_csv(
        os.path.join(INPUT_DIR, "co2/Primary energy (BP; WB; Shift).csv"),
        usecols=["Country", "Year", "Primary energy consumption (TWh) (BP & WB)"]
    )
    dataframe = dataframe.merge(energy, on=["Country", "Year"], how="left")
    return dataframe


def get_continents(dataframe):
    continents = pd.read_csv(
        os.path.join(INPUT_DIR, "shared/continents.csv"),
        usecols=["Country", "Region"]
    )
    continents = continents.merge(dataframe, on="Country")
    continents = (
        continents
        .groupby(["Region", "Year"], as_index=False)
        .sum()
        .rename(columns={"Region": "Country"})
        .replace(0, pd.NA)
    )
    return continents


def process_cdiac():
    # CDIAC data
    co2_cdiac = pd.read_csv(
        os.path.join(INPUT_DIR, "co2/co2_cdiac.csv"),
        skiprows=[1, 2, 3],
        na_values=[".", " "]
    )

    co2_cdiac = co2_cdiac.drop(columns=[
        "Emissions from solid fuel consumption",
        "Emissions from liquid fuel consumption",
        "Emissions from gas fuel consumption",
        "Emissions from cement production",
        "Emissions from gas flaring",
        "Per capita CO2 emissions (metric tons of carbon)",
        "Emissions from bunker fuels (not included in the totals)"
    ])

    co2_cdiac = co2_cdiac.rename(columns={
        "Total CO2 emissions from fossil-fuels and cement production (thousand metric tons of C)": \
            "CO2 emissions",
        "Nation": "Country"
    })

    co2_cdiac["Country"] = co2_cdiac["Country"].str.capitalize()

    # Convert from thousand tonnes of carbon to million tonnes of CO2
    cdiac_co2_conversion = 3.664 / 1000

    converted_cols = ["CO2 emissions"]
    co2_cdiac[converted_cols] = co2_cdiac[converted_cols].astype(float).mul(cdiac_co2_conversion)

    # Merge with country standardised names
    country_cdiac = pd.read_csv(os.path.join(INPUT_DIR, "shared/cdiac_country_standardized.csv"))
    co2_cdiac = co2_cdiac.merge(country_cdiac, on="Country")

    co2_cdiac = co2_cdiac.drop(columns=["Country"])
    co2_cdiac = co2_cdiac.rename(columns={"CountryStandardised": "Country"})

    co2_cdiac = co2_cdiac.groupby(["Country", "Year"], as_index=False).sum()
    return co2_cdiac


def process_gcp():
    country_gcp = pd.read_csv(os.path.join(INPUT_DIR, "shared/gcp_country_standardized.csv"))

    # GCP data
    co2_gcp = pd.read_excel(
        os.path.join(INPUT_DIR, "co2/co2_gcp.xlsx"),
        sheet_name="Territorial Emissions",
        skiprows=16
    )

    co2_gcp = pd.melt(
        co2_gcp,
        id_vars=["Year"],
        var_name=["Country"],
        value_name="CO2 emissions"
    )

    # Merge with country standardised names
    co2_gcp = (
        co2_gcp
        .merge(country_gcp, on="Country")
        .drop(columns=["Country"])
        .rename(columns={"CountryStandardised": "Country"})
    )
    converted_cols = ["CO2 emissions"]
    co2_gcp[converted_cols] = co2_gcp[converted_cols].astype(float).mul(GCP_CO2_CONVERSION)
    return co2_gcp


def add_global_emissions(dataframe):
    global_emissions = pd.read_csv(os.path.join(INPUT_DIR, "co2/global_co2.csv"))
    global_emissions = global_emissions.rename(columns={"Total emissions": "CO2 emissions"})
    dataframe = pd.concat([dataframe, global_emissions])
    return dataframe


def add_co2_peaked(dataframe):
    peak_emissions = dataframe.sort_values(["Country", "CO2 emissions"]).groupby("Country").tail(1)
    peak_emissions = peak_emissions[["Country", "Year"]].rename(columns={"Year": "peak_year"})
    dataframe = dataframe.merge(peak_emissions, on="Country", how="left")
    dataframe.loc[dataframe["Year"] < dataframe["peak_year"], "CO2_peaked"] = "False"
    dataframe.loc[dataframe["Year"] == dataframe["peak_year"], "CO2_peaked"] = "Highest year"
    dataframe.loc[dataframe["Year"] > dataframe["peak_year"], "CO2_peaked"] = "True"
    dataframe = dataframe.drop(columns=["peak_year"])
    return dataframe


def main():
    co2_cdiac = process_cdiac()
    co2_gcp = process_gcp()

    # Combine CDIAC and GCP annual CO2
    co2_cdiac.loc[:, "Source"] = "CDIAC"
    co2_cdiac.loc[:, "Priority"] = 0

    co2_gcp.loc[:, "Source"] = "GCP"
    co2_gcp.loc[:, "Priority"] = 1

    combined = (
        pd.concat([co2_cdiac, co2_gcp], join="outer")
        .sort_values(["Country", "Year", "Priority"])
        .groupby(["Year", "Country"])
        .tail(1)
    )

    # Drop columns
    combined = combined.drop(columns=["Priority", "Source"])

    # Reorder columns
    combined = combined[["Country", "Year", "CO2 emissions"]]

    combined = combined[-((combined["Country"] == "USSR") & (combined["Year"] > 1958))]

    # Add global figures
    combined = add_global_emissions(combined)

    # Add population, gdp, energy data
    combined = add_population(combined)
    combined = add_gdp(combined)
    combined = add_energy(combined)

    # Aggregate by region
    continents = get_continents(combined)

    region_fill = pd.read_csv(os.path.join(INPUT_DIR, "co2/regions_fill.csv"))

    combined = pd.concat([combined, continents, region_fill]).reset_index(drop=True)

    # Calculate annual growth rate
    combined = combined.sort_values(["Country", "Year"])
    combined["Annual CO2 growth (%)"] = (
        combined
        .groupby("Country")["CO2 emissions"]
        .pct_change() * 100
    )
    combined["Annual CO2 growth (abs)"] = combined.groupby("Country")["CO2 emissions"].diff()

    # Consumption-based emissions
    consumption = pd.read_excel(
        os.path.join(INPUT_DIR, "co2/co2_gcp.xlsx"),
        sheet_name="Consumption Emissions",
        skiprows=8
    )
    consumption = pd.melt(
        consumption,
        id_vars=["Year"],
        var_name=["Country"],
        value_name="Consumption emissions"
    )

    # Merge with country standardised names
    country_gcp = pd.read_csv(os.path.join(INPUT_DIR, "shared/gcp_country_standardized.csv"))
    consumption = (
        consumption
        .merge(country_gcp, on="Country")
        .drop(columns=["Country"])
        .rename(columns={"CountryStandardised": "Country"})
    )

    # Convert from million tonnes of carbon to million tonnes of CO2
    consumption[["Consumption emissions"]] = (
        consumption[["Consumption emissions"]]
        .astype(float)
        .mul(GCP_CO2_CONVERSION)
    )

    # Combine consumption-based with production-based
    combined = combined.merge(consumption, on=["Country", "Year"], how="left")
    combined["Traded emissions"] = combined["Consumption emissions"] - combined["CO2 emissions"]
    combined["Share emissions traded"] = (
        combined["Traded emissions"] / combined["CO2 emissions"]
        .mul(100)
    )

    # Calculating per capita emissions
    combined["Per capita emissions"] = (
        combined["CO2 emissions"] / combined["Population"]
        .mul(1000000)
    )
    combined["Per capita consumption emissions"] = (
        combined["Consumption emissions"] / combined["Population"]
        .mul(1000000)
    )

    # Drop population column
    combined = combined.drop(columns=["Population"])

    # Calculating the share of global emissions
    global_co2 = (
        pd.read_csv(
            os.path.join(INPUT_DIR, "co2/global_co2.csv"),
            usecols=["Year", "Total emissions"]
        ).rename(columns={"Total emissions": "Global emissions"})
    )

    combined = combined.merge(global_co2, on="Year", how="left")

    combined["Share of global emissions"] = (
        combined["CO2 emissions"] / combined["Global emissions"] * 100
    )

    # Calculating peak emissions
    combined = add_co2_peaked(combined)

    # Calculating cumulative emissions
    combined = combined.sort_values(["Country", "Year"])

    combined["Cumulative"] = combined.groupby("Country")["CO2 emissions"].cumsum()
    combined["Global cumulative"] = combined.groupby("Country")["Global emissions"].cumsum()

    # Calculating share of global cumulative emissions
    combined["Share of global cumulative emissions"] = (
        combined["Cumulative"] / combined["Global cumulative"]
        .mul(100)
    )

    # Calculating carbon intensity (per unit GDP)
    combined["CO2 per GDP (kg per $PPP)"] = (
        combined["CO2 emissions"] / combined["Total real GDP"]
        .mul(1000000000)
    )
    combined["Consumption-based CO2 per GDP (kg per $PPP)"] = (
        combined["Consumption emissions"] / combined["Total real GDP"]
        .mul(1000000000)
    )
    combined = combined.drop(columns=["Total real GDP"])

    # Calculating CO2 per unit energy
    combined["CO2 per unit energy (kgCO2 per kilowatt-hour)"] = (
        combined["CO2 emissions"] / combined["Primary energy consumption (TWh) (BP & WB)"]
    )
    combined = combined.drop(columns=[
        "Primary energy consumption (TWh) (BP & WB)",
        "Global cumulative",
        "Global emissions"
    ])

    # Clean up dataset for grapher
    combined = (
        combined
        .drop(columns=["CO2_peaked"])
        .replace([np.inf, -np.inf], np.nan)
        .dropna(subset=["Country"])
    )

    combined = combined.rename(columns={
        "CO2 emissions": "Annual CO2 emissions",
        "Consumption emissions": "Annual consumption-based CO2 emissions",
        "Traded emissions": "CO2 emissions embedded in trade",
        "Share emissions traded": "Share of CO2 emissions embedded in trade",
        "Per capita emissions": "Per capita CO2 emissions",
        "Per capita consumption emissions": "Per capita consumption-based CO2 emissions",
        "Share of global emissions": "Share of global CO2 emissions",
        "Cumulative": "Cumulative CO2 emissions",
        "Share of global cumulative emissions": "Share of global cumulative CO2 emissions"
    })

    # Save to CSV file
    combined.to_csv(
        os.path.join(GRAPHER_DIR, "co2_emissions.csv"),
        index=False
    )


if __name__ == '__main__':
    main()
