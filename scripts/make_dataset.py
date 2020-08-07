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

    combined = combined.rename(columns={
        "Country": "country",
        "Year": "year",
        "Annual CO2 emissions": "Annual CO2 emissions (Mt)",
        "Annual consumption-based CO2 emissions": "Annual consumption-based CO2 emissions (Mt)",
        "Annual CO2 growth (abs)": "Annual CO2 emisions growth (Mt)",
        "Annual CO2 growth (%)": "Annual growth in CO2 emissions (%)",
        "CO2 emissions embedded in trade": "CO2 emissions embedded in trade (Mt)",
        "Per capita CO2 emissions": "Per capita CO2 emissions (tonnes)",
        "Primary energy consumption (TWh) (BP & WB)": "Primary energy consumption (TWh)",
        "Per capita consumption-based CO2 emissions": \
            "Per capita consumption-based CO2 emissions (tonnes)",
        "Cumulative CO2 emissions": "Cumulative CO2 emissions (Mt)",
        "Cement": "CO2 emissions from cement (Mt)",
        "Coal": "CO2 emissions from coal (Mt)",
        "Oil": "CO2 emissions from oil (Mt)",
        "Gas": "CO2 emissions from gas (Mt)",
        "Flaring": "CO2 emissions from flaring (Mt)"
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
