import os
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(CURRENT_DIR, "input")
GRAPHER_DIR = os.path.join(CURRENT_DIR, "grapher")


def main():
    # Import raw emissions file from CAIT
    raw_data = pd.read_csv(os.path.join(INPUT_DIR, "ghg/historical_emissions.csv"))

    # Drop 'Data source' and 'Unit' columns
    raw_data = raw_data.drop(columns=["Data source", "Unit"])

    # Convert years into rows
    raw_data = raw_data.melt(
        id_vars=["Country", "Sector", "Gas"],
        var_name="Date",
        value_name="Value"
    )
    raw_data["Date"] = raw_data["Date"].astype(int)

    # Country mapping of OWID country names
    country_mapping = pd.read_csv(os.path.join(INPUT_DIR, "ghg/cait_country_standardized.csv"))

    # Import population dataset
    population = pd.read_csv(os.path.join(INPUT_DIR, "shared/population.csv"))

    for gas in ["All GHG", "CO2", "CH4", "N2O"]:

        gas_df = raw_data.loc[raw_data["Gas"] == gas]

        # Convert 'Sector' rows into separate columns
        # then put year on every row
        gas_df = gas_df.pivot_table(
            index=["Country", "Date"],
            columns="Sector",
            values="Value"
        ).reset_index()

        # Add country mapping of OWID country names
        gas_df = pd.merge(gas_df, country_mapping, on="Country")

        gas_df = gas_df.drop(columns=["Country"])

        # Rename columns
        gas_df = gas_df.rename(columns={
            "OWIDCountry": "Country",
            "Date": "Year",
            "Industrial Processes": "Industry",
            "Electricity/Heat": "Electricity & Heat",
            "Bunker Fuels": "International aviation & shipping",
            "Transportation": "Transport",
            "Manufacturing/Construction": "Manufacturing & Construction",
            "Building": "Buildings"
        })

        # Add population
        gas_df = gas_df.merge(population, how="left", on=["Country", "Year"])

        # Calculate per capita figures for all those columns if they exist in this version of gas_df
        columns_per_capita = [
            "Agriculture",
            "Buildings",
            "Electricity & Heat",
            "Fugitive Emissions",
            "Industry",
            "International aviation & shipping",
            "Land-Use Change and Forestry",
            "Manufacturing & Construction",
            "Total excluding LUCF",
            "Total including LUCF",
            "Transport",
            "Waste"
        ]
        for col in columns_per_capita:
            if col in gas_df.columns:
                gas_df[f"{col} (per capita)"] = gas_df[col] / gas_df["Population"] * 1000000

        # Drop 'Population' column
        gas_df = gas_df.drop(columns=["Population"])

        # Reorder columns
        left_columns = ["Country", "Year"]
        other_columns = sorted([col for col in gas_df.columns if col not in left_columns])
        column_order = left_columns + other_columns
        gas_df = gas_df[column_order]

        # Save files to CSV
        if gas == "All GHG":
            filename = "all_ghg_emissions.csv"
        else:
            filename = f"{gas}_by_sector.csv"
        gas_df.to_csv(os.path.join(GRAPHER_DIR, filename), index=False)


if __name__ == '__main__':
    main()
