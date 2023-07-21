#Optimized Code 1-(Time calculations of each chunk and total time taken, checking csv already processed)

#Code-1
# 1.[FETCH FINAL CODE]
#[Raw Data Fetched from telemetrydata api]
#Optimized faster processing

import os
import urllib.parse
import urllib.request
import json
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
import time
import io

def authenticate_drive(credentials_file):
    credentials = service_account.Credentials.from_service_account_file(credentials_file)
    return build('drive', 'v3', credentials=credentials)

def fetch_telemetry_data(start_id, id_interval, chunk_number, drive_service, folder_id):
    # Create file name
    csv_file_name = f"chunk{chunk_number:06}.csv"
    
    # REPLACE THE PATH WITH YOUR MAIN FOLDER {ML_Project_MHMM} & INSIDE IT THE SUB FOLDER {TelemetryData} TO STORE THE CSV
    csv_file_path = os.path.join('/content/drive/MyDrive/ML_Project_MHMM/TelemetryData', csv_file_name)

    # Check if the CSV file already exists on Google Drive
    file_exists = check_file_exists(drive_service, csv_file_name, folder_id)
    if file_exists:
        print(f"CSV file '{csv_file_name}' already exists on Google Drive. Skipping ID range: {start_id}/{start_id + id_interval - 1}")
        return

    # Fetch telemetry data
    url = "http://gapp.agverse.in/api/v1/get-telemetry-data"
    id_range = f"{start_id}/{start_id + id_interval - 1}"
    params = {"id": id_range}
    url_encoded_params = urllib.parse.urlencode(params)
    url_with_params = f"{url}?{url_encoded_params}"
    url_auth = urllib.request.Request(url_with_params, headers={"username": "gapp-apis", "password": "4Score&7yrsAgo"})

    try:
        start_time = time.time()
        with urllib.request.urlopen(url_auth) as response:
            data = response.read()
    except urllib.error.URLError as e:
        print("Error fetching telemetry data from the URL:", e)
        return False

    try:
        values = json.loads(data)
    except json.JSONDecodeError as e:
        print("Error decoding JSON data:", e)
        return False

    if values.get("status_code") == 200:
        telemetry_data = values.get("result", [])
        if telemetry_data:
            print(f"Telemetry data retrieved for ID range: {id_range}")
            sorted_data = sorted(telemetry_data, key=lambda x: x['id'])
            df = pd.DataFrame(sorted_data)

            # Convert DataFrame to CSV data
            csv_data = df.to_csv(index=False)

            # Upload the CSV file to Google Drive
            media_csv = MediaIoBaseUpload(io.BytesIO(csv_data.encode()), mimetype='text/csv', resumable=True)
            file_metadata_csv = {
                'name': csv_file_name,
                'parents': [folder_id]
            }
            try:
                file_csv = drive_service.files().create(body=file_metadata_csv, media_body=media_csv, fields='id').execute()
                end_time = time.time()
                csv_file_id = file_csv['id']  # Assign the file ID to csv_file_id
                print(f"CSV file uploaded to Google Drive with file ID: {csv_file_id}")
                chunk_time = end_time - start_time
                print(f"Time taken for chunk {chunk_number}: {chunk_time:.2f} seconds")
            except Exception as e:
                print("Error uploading CSV file to Google Drive:", e)
                return False
        else:
            print("No telemetry data found at the URL.")
            return False
    else:
        print("Error fetching telemetry data from the URL.")
        return False

    return True

def check_file_exists(drive_service, file_name, folder_id):
    response = drive_service.files().list(
        q=f"'{folder_id}' in parents and name = '{file_name}'",
        fields="files(name)",
        pageSize=1
    ).execute()

    files = response.get('files', [])
    return bool(files)

def main():
    start_id = 1
    id_interval = 100000
    folder_id = ' ' # REPLACE WITH YOUR FOLDER ID
    total_chunks = 77

    # Load the credentials from a JSON file
    credentials_file = '/content/drive/MyDrive/Credentials.json'  # Replace with your credentials file path

    # Authenticate Google Drive API
    drive_service = authenticate_drive(credentials_file)

    # Time taken calculated for each chunk and Total time taken
    chunk_times = []
    for chunk_number in range(1, total_chunks + 1):
        start_time = time.time()
        fetch_telemetry_data(start_id, id_interval, chunk_number, drive_service, folder_id)
        end_time = time.time()
        chunk_time = end_time - start_time
        chunk_times.append(chunk_time)
        start_id += id_interval

    total_time_minutes = sum(chunk_times) / 60

    print("Time taken for each chunk:")
    for i, chunk_time in enumerate(chunk_times):
        print(f"Chunk {i+1}: {chunk_time:.2f} seconds")

    print(f"\nTotal time taken: {sum(chunk_times):.2f} seconds")
    print(f"Total time taken: {total_time_minutes:.2f} minutes")

if __name__ == "__main__":
    main()

