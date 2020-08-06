import os
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(CURRENT_DIR, "input")
TMP_DIR = os.path.join(CURRENT_DIR, "tmp")


def main():

    # CDIAC
    df_cdiac = pd.read_csv(os.path.join(INPUT_DIR, "global_fuel/global_fuel_type.csv"), skiprows=4)
    df_cdiac = df_cdiac.drop(
        columns=["Per capita carbon emissions (metric tons of carbon; after 1949 only)"]
    )
    df_cdiac = df_cdiac.rename(columns={
        "Total carbon emissions from fossil fuel consumption and cement production (million metric tons of C)": "Total emissions",
        "Carbon emissions from solid fuel consumption": "Coal",
        "Carbon emissions from liquid fuel consumption": "Oil",
        "Carbon emissions from gas fuel consumption": "Gas",
        "Carbon emissions from cement production": "Cement",
        "Carbon emissions from gas flaring": "Flaring"
    })

    co2_conversion = 3.664

    converted_cols = ["Total emissions", "Coal", "Oil", "Gas", "Cement", "Flaring"]
    df_cdiac[converted_cols] = df_cdiac[converted_cols].astype(float).mul(co2_conversion)

    # Global Carbon Project
    df_gcp = pd.read_excel(
        os.path.join(INPUT_DIR, "global_fuel/global_fuel_type_gcp.xlsx"),
        sheet_name="Fossil Emissions by Fuel Type",
        skiprows=12
    )
    df_gcp = df_gcp.drop(columns=["Per Capita"])
    df_gcp = df_gcp.rename(columns={"Total": "Total emissions"})

    converted_cols = ["Total emissions", "Coal", "Oil", "Gas", "Cement", "Flaring"]
    df_gcp[converted_cols] = df_gcp[converted_cols].astype(float).mul(co2_conversion)

    # Merging
    df_cdiac.loc[:, "Source"] = "CDIAC"
    df_cdiac.loc[:, "Priority"] = 0

    df_gcp.loc[:, "Source"] = "GCP"
    df_gcp.loc[:, "Priority"] = 1

    combined = pd.concat([df_cdiac, df_gcp])
    combined = combined.sort_values(["Year", "Priority"])
    combined = combined.groupby(["Year"]).tail(1)
    combined = combined.drop(columns=["Source", "Priority"])

    combined.insert(0, "Country", "World")

    # Save to CSV file
    combined.to_csv(os.path.join(TMP_DIR, "global_co2_fuel_type.csv"), index=False)


if __name__ == "__main__":
    main()
