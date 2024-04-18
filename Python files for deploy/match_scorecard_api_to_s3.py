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
S3_UPLOADED_FILE_NAME = 'match_scorecard'

match_arr = [2844251, 2844253, 2844255, 2844257, 2844259, 2844261, 2844263, 2844265, 
             2844267, 2844269, 2844271, 2844273, 2844275, 2844277, 2844279,
             2844281, 2844283, 2844285, 2844287, 2844289]

def fetch_match_data(match_ids):
    entry = []
    for num in match_ids:
        url = f'https://cricket-live-data.p.rapidapi.com/match/{num}'
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": RAPIDAPI_HOST
        }
        response = requests.get(url, headers=headers)
        json_data = response.json()
        ide = json_data['results']['fixture']['id']
        series_id = json_data['results']['fixture']['series_id']
        match_title = json_data['results']['fixture']['match_title']
        venue = json_data['results']['fixture']['venue']
        start_date = json_data['results']['fixture']['start_date']
        end_date = json_data['results']['fixture']['end_date']
        home = json_data['results']['fixture']['home']['id']
        away = json_data['results']['fixture']['away']['id']
        try:
            result = json_data['results']['live_details']['match_summary']['status']
        except:
            result = "Match not completed"
        record = [ide, series_id, match_title, venue, start_date, end_date, home, away, result]
        entry.append(record)
    return pd.DataFrame(entry, columns=['id', 'series_id', 'match_title', 'venue', 'start_date', 
                                        'end_date', 'home_team_id', 'away_team_id', 'result'])

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

# Fetch match data
df_match = fetch_match_data(match_arr)

cols = ['match_title', 'venue', 'result']

for col in cols:
    df_match[col] = df_match[col].str.replace(',', '')


# Upload to S3
upload_to_s3(df_match, S3_BUCKET_NAME, S3_UPLOADED_FILE_NAME)
