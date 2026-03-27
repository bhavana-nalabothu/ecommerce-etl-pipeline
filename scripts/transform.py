import pandas as pd

def transform_data(df):

    # Remove null values
    df = df.dropna()

    # Clean Amount column (remove $ and ,)
    df['Amount'] = df['Amount'].replace('[\$,]', '', regex=True).astype(float)

    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%y')

    # Create new column: price per box
    df['price_per_box'] = df['Amount'] / df['Boxes Shipped']

    print("Data transformed successfully!")
    print(df.head())

    return df


# Test
if __name__ == "__main__":
    from extract import extract_from_s3

    bucket = "ecommerce-etl-bucket-bhavana-303575244299-us-east-2-an"
    file_key = "Chocolate Sales.csv"

    df = extract_from_s3(bucket, file_key)
    transform_data(df)

