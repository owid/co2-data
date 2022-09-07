"""Upload OWID CO2 dataset files to S3 and make them publicly available.

This script requires OWID credentials to write files in the S3 bucket.

Files should be accessible at the following urls:
* https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv
* https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.xlsx
* https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.json

"""

import argparse
from pathlib import Path

from tqdm.auto import tqdm
from owid.datautils.io.s3 import S3

from scripts import OUTPUT_DIR

# Define S3 base URL.
S3_URL = "https://nyc3.digitaloceanspaces.com"
# Profile name to use for S3 client (as defined in .aws/config).
S3_PROFILE_NAME = "default"
# S3 bucket name and folder where dataset files will be stored.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/co2")
# Local files to upload.
FILES_TO_UPLOAD = {
    OUTPUT_DIR / "owid-co2-data.csv": S3_DATA_DIR / "owid-co2-data.csv",
    OUTPUT_DIR / "owid-co2-data.json": S3_DATA_DIR / "owid-co2-data.json",
    OUTPUT_DIR / "owid-co2-data.xlsx": S3_DATA_DIR / "owid-co2-data.xlsx",
}


def main(files_to_upload, s3_bucket_name=S3_BUCKET_NAME):
    # Make files publicly available.
    public = True
    # Initialise S3 client.
    s3 = S3()
    # Upload and make public each of the files.
    for local_file in tqdm(files_to_upload):
        s3_file = files_to_upload[local_file]
        tqdm.write(
            f"Uploading file {local_file} to S3 bucket {s3_bucket_name} as {s3_file}."
        )
        s3.upload_to_s3(
            local_path=str(local_file),
            s3_path=f"s3://{s3_bucket_name}/{str(s3_file)}",
            public=public,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload OWID co2 dataset files to S3, and make them publicly readable."
    )
    args = parser.parse_args()

    main(files_to_upload=FILES_TO_UPLOAD)
