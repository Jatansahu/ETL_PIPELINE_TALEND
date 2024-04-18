import pandas as pd
import requests
import boto3
from io import StringIO
import datetime

# Global variables
RAPIDAPI_KEY = "f381eaaa79msh51537dd6e014035p180695jsn67cf7d549118"
RAPIDAPI_HOST = "cricket-live-data.p.rapidapi.com"
AWS_ACCESS_KEY_ID = 'AKIARAACMVC5URUSKCGX'
AWS_SECRET_ACCESS_KEY = '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'
S3_BUCKET_NAME = 'cricket-api-to-s3'
S3_UPLOADED_FILE_NAME = 'results-by-date'

# API endpoint base URL and headers
base_url = "https://cricket-live-data.p.rapidapi.com/results-by-date/"
headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": RAPIDAPI_HOST
}
def fetch_result_by_date(date_list):
    df_date = []
    for date in date_list:
        url = base_url + date
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            for i in range(len(json_data['results'])):
                entry = []
                ide = json_data['results'][i]['id']
                series_id = json_data['results'][i]['series_id']
                venue = json_data['results'][i]['venue']
                date_with_timestamp = json_data['results'][i]['date']
                status = json_data['results'][i]['status']
                result = json_data['results'][i]['result']
                match_title = json_data['results'][i]['match_title']
                match_subtitle = json_data['results'][i]['match_subtitle']
                home_team_id = json_data['results'][i]['home']['id']
                away_team_id = json_data['results'][i]['away']['id']
                entry.append([ide, series_id, venue, date_with_timestamp, status, result, match_title, match_subtitle,
                        home_team_id, away_team_id])
                df_date.extend(entry)
    return pd.DataFrame(df_date, columns = ['id', 'series_id', 'venue', 'date_with_timestamp', 'status', 'result',
                                           'match_title', 'match_subtitle', 'home_team_id', 'away_team_id'])


# Function to save DataFrame to CSV and upload to S3
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

# List of dates
arr_datess = ['2024-03-19', '2024-03-20', '2024-03-21','2024-03-22', '2024-03-23', 
              '2024-03-23', '2024-03-24', '2024-03-24', '2024-03-25', '2024-03-26', 
              '2024-03-27', '2024-03-28', '2024-03-29', '2024-03-30', '2024-03-31', 
              '2024-03-31', '2024-04-01']

# Fetch data by date
df_result_by_date = fetch_result_by_date(arr_datess)

cols = ['venue', 'result', 'match_title', 'match_subtitle']
for col in cols:
    df_result_by_date[col] = df_result_by_date[col].str.replace(',', '')

# Upload to S3
upload_to_s3(df_result_by_date, S3_BUCKET_NAME, S3_UPLOADED_FILE_NAME)
