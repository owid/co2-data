import os
import json
import pandas as pd
import numpy as np


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

    # kludge: renames countries in the primary_energy dataset prior to merge.
    primary_energy['Country'].replace({
        'Burma': 'Myanmar',
        'Ivory Coast': 'Côte d\'Ivoire',
        'Macedonia': 'North Macedonia'
    }, inplace=True)

    # replaces values of 0 with NaN in the co2 dataset, b/c most (all?) of 
    # these 0 values reflect missing data rather than an actual 0 value.
    co2 = co2.replace(0, np.nan)
    
    # merges together the emissions datasets
    combined = (
        co2
        .merge(ghg_emissions, on=["Year", "Country"], how="outer", validate="1:1")
        .merge(ch4, on=["Year", "Country"], how="outer", validate="1:1")
        .merge(n2o, on=["Year", "Country"], how="outer", validate="1:1")
    )
    
    # drops any country-year rows that contain only NaN values.
    row_has_data = combined.drop(columns=['Country', 'Year']).notnull().any(axis=1)
    combined = combined[row_has_data]

    # merges non-emissions datasets onto emissions dataset
    combined = (
        combined
        .merge(primary_energy, on=["Year", "Country"], how="left", validate="1:1")
        .merge(population, on=["Year", "Country"], how="left", validate="1:1")
        .merge(gdp, on=["Year", "Country"], how="left", validate="1:1")
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
        "Per capita consumption-based CO2 emissions": "consumption_co2_per_capita",
        "Share of global CO2 emissions": "share_global_co2",
        "Cumulative CO2 emissions": "cumulative_co2",
        "Share of global cumulative CO2 emissions": "share_global_cumulative_co2",
        "CO2 per GDP (kg per $PPP)": "co2_per_gdp",
        "Consumption-based CO2 per GDP (kg per $PPP)": "consumption_co2_per_gdp",
        "CO2 per unit energy (kgCO2 per kilowatt-hour)": "co2_per_unit_energy",
        "CO2 emissions from cement": "cement_co2",
        "CO2 emissions from coal": "coal_co2",
        "CO2 emissions from oil": "oil_co2",
        "CO2 emissions from gas": "gas_co2",
        "CO2 emissions from flaring": "flaring_co2",
        "CO2 emissions from other industry": "other_industry_co2",
        "Cement emissions (per capita)": "cement_co2_per_capita",
        "Coal emissions (per capita)": "coal_co2_per_capita",
        "Oil emissions (per capita)": "oil_co2_per_capita",
        "Gas emissions (per capita)": "gas_co2_per_capita",
        "Flaring emissions (per capita)": "flaring_co2_per_capita",
        "Other emissions (per capita)": "other_co2_per_capita",
        "Share of global coal emissions": "share_global_coal_co2",
        "Share of global oil emissions": "share_global_oil_co2",
        "Share of global gas emissions": "share_global_gas_co2",
        "Share of global flaring emissions": "share_global_flaring_co2",
        "Share of global cement emissions": "share_global_cement_co2",
        "Cumulative coal emissions": "cumulative_coal_co2",
        "Cumulative oil emissions": "cumulative_oil_co2",
        "Cumulative gas emissions": "cumulative_gas_co2",
        "Cumulative flaring emissions": "cumulative_flaring_co2",
        "Cumulative cement emissions": "cumulative_cement_co2",
        "Cumulative other industry emissions": "cumulative_other_co2",
        "Share of global cumulative coal emissions": "share_global_cumulative_coal_co2",
        "Share of global cumulative oil emissions": "share_global_cumulative_oil_co2",
        "Share of global cumulative gas emissions": "share_global_cumulative_gas_co2",
        "Share of global cumulative flaring emissions": "share_global_cumulative_flaring_co2",
        "Share of global cumulative cement emissions": "share_global_cumulative_cement_co2",
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
    
    # drops all columns that are not in the codebook
    codebook = pd.read_csv(os.path.join(OUTPUT_DIR, "owid-co2-codebook.csv"))
    combined = combined[codebook['column'].tolist()]
    
    # reorders columns so that index columns are first.
    combined = combined.set_index(['iso_code', 'country', 'year']).reset_index()

    combined.to_csv(
        os.path.join(OUTPUT_DIR, "owid-co2-data.csv"), index=False
    )
    combined.to_excel(
        os.path.join(OUTPUT_DIR, "owid-co2-data.xlsx"), index=False
    )
    df_to_json(combined, os.path.join(OUTPUT_DIR, "owid-co2-data.json"), ["iso_code"])


if __name__ == "__main__":
    main()
