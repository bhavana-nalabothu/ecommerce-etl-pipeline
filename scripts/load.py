import pandas as pd
import psycopg2
from extract import extract_from_s3

# CONFIG
bucket_name = "*******************************************"
file_key = "**************************"

db_user = "postgres"
db_password = "****************"
db_host = "ecommerce-db.cvmguwio8spk.us-east-2.rds.amazonaws.com"
db_port = "5432"
db_name = "ecommerce_db"

table_name = "sales_data"

# LOAD FUNCTION
def load_to_rds(df):
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )

        cursor = conn.cursor()
        print("Connected to RDS successfully!")

        # Create table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {sales_data} (
            sales_person TEXT,
            country TEXT,
            product TEXT,
            sale_date DATE,
            amount FLOAT,
            boxes_shipped INT
        );
        """
        cursor.execute(create_table_query)

        # Insert data
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {sales_data}
            (sales_person, country, product, sale_date, amount, boxes_shipped)
            VALUES (%s, %s, %s, %s, %s, %s);
            """

            cursor.execute(insert_query, (
                row['Sales Person'],
                row['Country'],
                row['Product'],
                row['Date'],
                float(str(row['Amount']).replace('$', '').replace(',', '')),
                int(row['Boxes Shipped'])
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print("Data loaded into RDS successfully!")

    except Exception as e:
        print("Error:", e)


# MAIN
if __name__ == "__main__":
    df = extract_from_s3("ecommerce-etl-bucket-bhavana-303575244299-us-east-2-an", "Chocolate Sales.csv")
    load_to_rds(df)

