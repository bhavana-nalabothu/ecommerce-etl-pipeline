import boto3
import pandas as pd

def extract_from_s3(bucket_name, file_key):
    # Create S3 client
    s3 = boto3.client('s3')

    # Download CSV from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)

    # Read into pandas dataframe
    df = pd.read_csv(response['Body'])

    print("Data extracted successfully!")
    print(df.head())

    return df

if __name__ == "__main__":
    bucket ="ecommerce-etl-bucket-bhavana-303575244299-us-east-2-an"       
    file_key = "Chocolate Sales.csv"       

    extract_from_s3(bucket, file_key)
