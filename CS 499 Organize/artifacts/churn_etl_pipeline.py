# churn_etl_pipeline.py

import os
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Retrieve AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION")

# S3 and local path configuration
BUCKET = "churnanalysismihir"
KEY = "CustomerChurnAnalysis.csv"
LOCAL_PATH = os.path.join(os.path.expanduser("~"), "churn_data_raw.csv")  # Save file to home directory

def download_from_s3(bucket, key, destination):
    """
    Download a file from AWS S3 to the specified local path.
    """
    try:
        # Create an S3 session with credentials
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION,
        )
        s3 = session.client("s3")
        
        # Attempt to download the file
        logging.info(f"Downloading s3://{bucket}/{key} to {destination}")
        s3.download_file(bucket, key, destination)
        logging.info("Download complete.")
        return True
    except ClientError as e:
        # Log error if download fails
        logging.error(f"AWS ClientError: {e.response['Error']['Message']}")
        return False

def load_and_clean_data(path):
    """
    Load CSV data from the given path and perform basic cleaning:
    - Normalize column names
    - Drop rows with missing values
    """
    try:
        df = pd.read_csv(path)
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]  # Normalize column names
        df.dropna(inplace=True)  # Remove rows with missing values
        return df
    except Exception as e:
        logging.error(f"Failed to load or clean data: {str(e)}")
        return pd.DataFrame()

def main():
    """
    Main execution function:
    - Download data from S3
    - Clean and load data
    - Display preview
    """
    if download_from_s3(BUCKET, KEY, LOCAL_PATH):
        df = load_and_clean_data(LOCAL_PATH)
        print(df.head())  # Display first few rows
    else:
        print("Data download failed.")

# Run main if this script is executed directly
if __name__ == "__main__":
    main()
