import pandas as pd
import requests
import boto3
from io import StringIO
import datetime

# Global variables
RAPIDAPI_KEY = "c8b54ea61cmshd698d020df02156p1b6e01jsn246fa3374c64"
RAPIDAPI_HOST = "cricket-live-data.p.rapidapi.com"
AWS_ACCESS_KEY_ID = 'AKIARAACMVC5URUSKCGX'
AWS_SECRET_ACCESS_KEY = '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'
S3_BUCKET_NAME = 'cricket-api-to-s3'
S3_FOLDER_NAME = 'fixtures_by_date'

# API endpoint base URL and headers
base_url = "https://cricket-live-data.p.rapidapi.com/fixtures-by-date/"
headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": RAPIDAPI_HOST
}


arr_date = ['2023-04-02', '2023-04-03', '2023-04-04', '2023-04-05', '2024-03-22', '2024-03-23', '2024-03-23', '2024-03-24', '2024-03-24', '2024-03-25', '2024-03-26', '2024-03-27', '2024-03-28', '2024-03-29', '2024-03-30', '2024-03-31', '2024-03-31', '2024-04-01', '2024-04-02', '2024-04-03']


def fetch_fixtures_by_date(arr_date):
    df_date = []
    for date in arr_date:
        url = base_url + date
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            json_data = response.json()
            fix_data = json_data.get('results', [])
            df_date.extend(fix_data)
        else:
            print("Failed to fetch data for date:", date)
    return pd.DataFrame(df_date)

# Function to save DataFrame to CSV and upload to S3
def upload_to_s3(df, bucket_name, folder, file_name):
    current_date = datetime.datetime.now()
    year = current_date.year
    month = current_date.month

    s3_output_name = f"{folder}/{year}/{month:02}/{file_name}_{year}_{month:02}_{current_date.day}.csv"
    

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=s3_output_name)
    
    print(f"Data has been uploaded to S3 bucket {bucket_name} with key {s3_output_name}")

df_fixtures_by_date = fetch_fixtures_by_date(arr_date)

upload_to_s3(df_fixtures_by_date, S3_BUCKET_NAME, S3_FOLDER_NAME, S3_FOLDER_NAME)
