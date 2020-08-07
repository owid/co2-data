import os
import json
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(CURRENT_DIR, "input")
GRAPHER_DIR = os.path.join(CURRENT_DIR, "grapher")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "..")


def add_iso_codes(dataframe):
    iso_codes = pd.read_csv(os.path.join(INPUT_DIR, "shared/iso3166_1_alpha_3_codes.csv"))
    dataframe = iso_codes.merge(dataframe, on="country", how="right")
    return dataframe


def df_to_json(complete_dataset, output_path, static_columns):
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


def main():
    # Add CO2 emissions
    co2 = pd.read_csv(os.path.join(GRAPHER_DIR, "co2_emissions.csv"))

    # Add CO2 by fuel type
    co2_by_fuel = pd.read_csv(os.path.join(GRAPHER_DIR, "co2_fuel.csv"))

    # Add GHG emissions data
    ghg_emissions = pd.read_csv(
        os.path.join(GRAPHER_DIR, "all_ghg_emissions.csv"),
        usecols=["Country", "Year", "Total including LUCF", "Total including LUCF (per capita)"]
    )
    ghg_emissions = ghg_emissions.rename(columns={
        "Total including LUCF": "Total GHG emissions (MtCO2e)",
        "Total including LUCF (per capita)": "GHG emissions per capita (tCO2e)"
    })

    ch4 = pd.read_csv(
        os.path.join(GRAPHER_DIR, "CH4_by_sector.csv"),
        usecols=["Country", "Year", "Total including LUCF", "Total including LUCF (per capita)"]
    )
    ch4 = ch4.rename(columns={
        "Total including LUCF": "CH4 emissions (MtCO2e)",
        "Total including LUCF (per capita)": "CH4 emissions per capita (tCO2e)"
    })

    n2o = pd.read_csv(
        os.path.join(GRAPHER_DIR, "N2O_by_sector.csv"),
        usecols=[
            "Country",
            "Year",
            "Total including LUCF",
            "Total including LUCF (per capita)"
        ]
    )
    n2o = n2o.rename(columns={
        "Total including LUCF": "N2O emissions (MtCO2e)",
        "Total including LUCF (per capita)": "N2O emissions per capita (tCO2e)"
    })

    # Add primary energy data
    primary_energy = pd.read_csv(
        os.path.join(INPUT_DIR, "primary_energy/primary_energy.csv"),
        usecols=[
            "Country", "Year", "Primary energy consumption (TWh) (BP & WB)",
            "Energy per capita (kWh)", "Energy per GDP (kWh per $)"
        ]
    )

    # Add population and GDP data
    population = pd.read_csv(os.path.join(INPUT_DIR, "shared/population.csv"))
    gdp = pd.read_csv(
        os.path.join(INPUT_DIR, "shared/total-gdp-maddison.csv"),
        usecols=["Entity", "Year", "Total real GDP"]
    ).rename(columns={"Entity": "Country"})

    # Combine datasets
    combined = (
        co2
        .merge(co2_by_fuel, on=["Year", "Country"], how="left")
        .merge(ghg_emissions, on=["Year", "Country"], how="left")
        .merge(ch4, on=["Year", "Country"], how="left")
        .merge(n2o, on=["Year", "Country"], how="left")
        .merge(primary_energy, on=["Year", "Country"], how="left")
        .merge(population, on=["Year", "Country"], how="left")
        .merge(gdp, on=["Year", "Country"], how="left")
    )

    combined = combined.rename(errors="raise", columns={
        "Country": "country",
        "Year": "year",
        "Annual CO2 emissions": "co2",
        "Annual consumption-based CO2 emissions": "consumption_co2",
        "Annual CO2 growth (abs)": "co2_growth_abs",
        "Annual CO2 growth (%)": "co2_growth_prct",
        "CO2 emissions embedded in trade": "trade_co2",
        "Share of CO2 emissions embedded in trade": "trade_co2_share",
        "Per capita CO2 emissions": "co2_per_capita",
        "Primary energy consumption (TWh) (BP & WB)": "Primary energy consumption (TWh)",
        "Per capita consumption-based CO2 emissions": "consumption_co2_per_capita",
        "Share of global CO2 emissions": "share_global_co2",
        "Cumulative CO2 emissions": "cumulative_co2",
        "Share of global cumulative CO2 emissions": "share_global_cumulative_co2",
        "CO2 per GDP (kg per $PPP)": "co2_per_gdp",
        "Consumption-based CO2 per GDP (kg per $PPP)": "consumption_co2_per_gdp",
        "CO2 per unit energy (kgCO2 per kilowatt-hour)": "co2_per_unit_energy",
        "Cement": "cement_co2",
        "Coal": "coal_co2",
        "Oil": "oil_co2",
        "Gas": "gas_co2",
        "Flaring": "flaring_co2",
        "Cement (per capita)": "cement_co2_per_capita",
        "Coal (per capita)": "coal_co2_per_capita",
        "Oil (per capita)": "oil_co2_per_capita",
        "Gas (per capita)": "gas_co2_per_capita",
        "Flaring (per capita)": "flaring_co2_per_capita",
        "Total GHG emissions (MtCO2e)": "total_ghg",
        "GHG emissions per capita (tCO2e)": "ghg_per_capita",
        "CH4 emissions (MtCO2e)": "methane",
        "CH4 emissions per capita (tCO2e)": "methane_per_capita",
        "N2O emissions (MtCO2e)": "nitrous_oxide",
        "N2O emissions per capita (tCO2e)": "nitrous_oxide_per_capita",
        "Primary energy consumption (TWh) (BP & WB)": "primary_energy_consumption",
        "Energy per capita (kWh)": "energy_per_capita",
        "Energy per GDP (kWh per $)": "energy_per_gdp",
        "Population": "population",
        "Total real GDP": "gdp"
    })

    combined = add_iso_codes(combined)
    combined = combined.round(3)
    combined = combined.sort_values(["country", "year"])

    combined.to_csv(
        os.path.join(OUTPUT_DIR, "owid-co2-data.csv"), index=False
    )
    combined.to_excel(
        os.path.join(OUTPUT_DIR, "owid-co2-data.xlsx"), index=False
    )
    df_to_json(combined, os.path.join(OUTPUT_DIR, "owid-co2-data.json"), ["iso_code"])


if __name__ == "__main__":
    main()
