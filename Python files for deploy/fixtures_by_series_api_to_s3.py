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
S3_UPLOADED_FILE_NAME = 'fixtures_by_series'

base_url = "https://cricket-live-data.p.rapidapi.com/fixtures-by-series/"
headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": RAPIDAPI_HOST
}

def fetch_fixtures_by_series(date_list):
    df_date = []
    df_venue = []
    for date in date_list:
        url = base_url + str(date)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            for i in range(len(json_data['results'])):
                entry = []
                entry_venue = []
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
                venue_id = json_data['results'][i]['home']['id']
                name = json_data['results'][i]['home']['name']
                code = json_data['results'][i]['home']['code']

                entry.append([ide, series_id, venue, date_with_timestamp, status, result, match_title, match_subtitle,
                        home_team_id, away_team_id])
                entry_venue.append([venue_id, name, code])

                df_date.extend(entry)
                df_venue.extend(entry_venue)

    return (pd.DataFrame(df_date, columns = ['id', 'series_id', 'venue', 'date_with_timestamp', 'status', 'result', 
            'match_title', 'match_subtitle', 'home_team_id', 'away_team_id']),
            pd.DataFrame(df_venue, columns = ['id', 'name', 'code']))



def upload_to_s3(df, bucket_name, S3_UPLOADED_FILE_NAME):
    current_date = datetime.datetime.now()
    year = current_date.year
    month = current_date.month

  
    s3_output_name = f"{S3_UPLOADED_FILE_NAME}/" 
    s3_output_name += f"{year}/" 
    s3_output_name += f"{month:02}/"  
    s3_output_name += f"{S3_UPLOADED_FILE_NAME}_{year}_{month:02}_{current_date.day}.csv"  

    # Save DataFrame to CSV and upload to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=s3_output_name)
    print(f"Data has been uploaded to S3 bucket {bucket_name} with key {s3_output_name}")




series_ids = [2002, 1430, 978, 812, 833, 608, 756, 425]


fixtures_df = fetch_fixtures_by_series(series_ids)[0]
venue_df = fetch_fixtures_by_series(series_ids)[1]

cols = ['venue', 'result', 'match_title', 'match_subtitle']
for col in cols:
    fixtures_df[col] = fixtures_df[col].str.replace(',', '')

cols = ['name']
for col in cols:
    venue_df[col] = venue_df[col].str.replace(',', '')

upload_to_s3(fixtures_df, S3_BUCKET_NAME, S3_UPLOADED_FILE_NAME)
upload_to_s3(venue_df, S3_BUCKET_NAME, 'Venue')