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
S3_FOLDER_NAME = 'results'


def fetch_data_from_api(url):
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        json_data = response.json()
        df_date = []
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
    else:
        print("Failed to fetch data from:", url)
        return None

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


results_df = fetch_data_from_api("https://cricket-live-data.p.rapidapi.com/results")

cols = ['venue', 'result', 'match_title', 'match_subtitle']
for col in cols:
    results_df[col] = results_df[col].str.replace(',', '')


# Upload to S3
if results_df is not None:
    upload_to_s3(results_df, S3_BUCKET_NAME, S3_FOLDER_NAME, S3_FOLDER_NAME)
