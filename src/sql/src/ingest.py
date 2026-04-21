import boto3
import pandas as pd
from datetime import datetime

def ingest_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read raw data from S3 into a pandas DataFrame."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])
    print(f"Ingested {len(df)} records from s3://{bucket}/{key}")
    return df

def save_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """Save processed DataFrame back to S3 as Parquet."""
    local_path = f"/tmp/{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(local_path, index=False)

    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    print(f"Saved {len(df)} records to s3://{bucket}/{key}")

if __name__ == "__main__":
    df = ingest_from_s3("my-bucket", "raw/metrics/data.csv")
    save_to_s3(df, "my-bucket", "silver/metrics/data.parquet")
