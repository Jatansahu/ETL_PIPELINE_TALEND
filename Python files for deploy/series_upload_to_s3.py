import pandas as pd
import requests
import boto3
from io import StringIO
import datetime

# Global variables
RAPIDAPI_KEY = "b72f452b32msheb263a2dd53ade5p151fa5jsn1016eed39cfc"
RAPIDAPI_HOST = "cricket-live-data.p.rapidapi.com"
AWS_ACCESS_KEY_ID = 'AKIARAACMVC5URUSKCGX'
AWS_SECRET_ACCESS_KEY = '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'
S3_BUCKET_NAME = 'cricket-api-to-s3'
S3_UPLOADED_FILE_NAME = 'series'

# API endpoint function
def fetch_data_from_api(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data:", response.status_code)
        return None

# Function to process data
def process_data(json_data):
    all_series_data = []
    for result in json_data['results']:
        series_data = result['series']
        series_type = result['type']
        # Add 'type' to each series data
        for data in series_data:
            data['type'] = series_type
        all_series_data.extend(series_data)
    return pd.DataFrame(all_series_data)

# # Function to save DataFrame to CSV and upload to S3
# def upload_to_s3(df, bucket_name, s3_output_name):
#     csv_buffer = StringIO()
#     df.to_csv(csv_buffer, index=False)
#     s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#     s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=s3_output_name)
#     print(f"Data has been uploaded to S3 bucket {bucket_name} with key {s3_output_name}")


def upload_to_s3(df, bucket_name, folder):
    # Get current year and month
    current_date = datetime.datetime.now()
    year = current_date.year
    month = current_date.month

    # Construct S3 key
    s3_output_name = f"{folder}/" 
    s3_output_name += f"{year}/" 
    s3_output_name += f"{month:02}/"  
    s3_output_name += f"{folder}_{year}_{month:02}_{current_date.day}.csv"  

    # Save DataFrame to CSV and upload to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=s3_output_name)
    print(f"Data has been uploaded to S3 bucket {bucket_name} with key {s3_output_name}")


# Main function
def main():
    url = "https://cricket-live-data.p.rapidapi.com/series"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }
    json_data = fetch_data_from_api(url, headers)
    if json_data:
        df_series = process_data(json_data)
        cols = ['series_name', 'status']
        for col in cols:
            df_series[col] = df_series[col].str.replace(',', '')
        upload_to_s3(df_series, S3_BUCKET_NAME, S3_UPLOADED_FILE_NAME)
    else:
        print("No data fetched from the API.")

if __name__ == "__main__":
    main()
