"""Upload OWID CO2 dataset files to S3 and make them publicly available.

This script requires OWID credentials to write files in the S3 bucket.

Files should be accessible at the following urls:
* https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv
* https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.xlsx
* https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.json

"""

import argparse
import os

import boto3
from tqdm.auto import tqdm

# Define path to current directory.
CURRENT_DIR = os.path.dirname(__file__)
# Define S3 base URL.
S3_URL = "https://nyc3.digitaloceanspaces.com"
# Profile name to use for S3 client (as defined in .aws/config).
S3_PROFILE_NAME = "default"
# S3 bucket name and folder where dataset files will be stored.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = os.path.join("data", "co2")
# Local files to upload.
FILES_TO_UPLOAD = {
    os.path.join(CURRENT_DIR, "..", "owid-co2-data.csv"): os.path.join(
        S3_DATA_DIR, "owid-co2-data.csv"
    ),
    os.path.join(CURRENT_DIR, "..", "owid-co2-data.json"): os.path.join(
        S3_DATA_DIR, "owid-co2-data.json"
    ),
    os.path.join(CURRENT_DIR, "..", "owid-co2-data.xlsx"): os.path.join(
        S3_DATA_DIR, "owid-co2-data.xlsx"
    ),
}


class S3:
    def __init__(self, s3_url=S3_URL, s3_profile_name=S3_PROFILE_NAME):
        """Minimum implementation of an S3 interaction object.

        Parameters
        ----------
        s3_url : str
            S3 base URL.
        s3_profile_name : str
            Profile name to use for S3 client (as defined in .aws/config).

        """
        self.s3_url = s3_url
        self.s3_profile_name = s3_profile_name
        # Initialise client.
        self.session = boto3.Session(profile_name=self.s3_profile_name)
        self.client = self.session.client(service_name="s3", endpoint_url=self.s3_url)

    def list_files_in_folder(self, bucket_name, path_to_folder):
        """List files in a folder within a bucket.

        Parameters
        ----------
        bucket_name : str
            Bucket name.
        path_to_folder : str
            Path (within the bucket) to folder.

        Returns
        -------
        objects_list : list
            Objects found in folder.

        """
        objects_request = self.client.list_objects_v2(
            Bucket=bucket_name, Prefix=path_to_folder
        )
        # List all objects with a prefix starting like the given path.
        # Remove the first element (which is the folder itself).
        objects_list = [obj["Key"] for obj in objects_request["Contents"]][1:]

        return objects_list

    def upload_file(self, local_file, bucket_name, s3_file, public=False):
        """Upload local file to a certain folder in an S3 bucket.

        Parameters
        ----------
        local_file : str
            Path to local file to be uploaded.
        bucket_name : str
            Bucket name.
        s3_file : str
            Path (within bucket) to end file.
            NOTE: It cannot be just the containing folder, it must be the name of the file to be created.
        public : bool
            True to make the file publicly readable.

        """
        extra_args = {"ACL": "public-read"} if public else {}
        self.client.upload_file(
            Filename=local_file, Bucket=bucket_name, Key=s3_file, ExtraArgs=extra_args
        )

    def download_file(self, s3_file, bucket_name, local_file):
        """Download file from S3 bucket.

        Parameters
        ----------
        s3_file : str
            Path (within bucket) of file to be downloaded.
        bucket_name : str
            Bucket name.
        local_file : str
            Path to local file.
            NOTE: It cannot be just the containing folder, it must be the name of the file to be created.

        Returns
        -------
        downloaded_file : object
            Downloaded file.

        """
        downloaded_file = self.client.download_file(
            Bucket=bucket_name, Key=s3_file, Filename=local_file
        )

        return downloaded_file


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
        s3.upload_file(
            local_file=local_file,
            bucket_name=s3_bucket_name,
            s3_file=s3_file,
            public=public,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload OWID co2 dataset files to S3, and make them publicly readable."
    )
    args = parser.parse_args()

    main(files_to_upload=FILES_TO_UPLOAD)
