#2. FULL TELEMETRY_DATA COMPATIABLE CODE
#(FINAL CODE)-ANIMALDATA (animal_id.csv)-vedba_converted,signed_value,vedba_value,vedba_value_encoded generated

#####################################################################################################################

import csv
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import numpy as np
from googleapiclient.http import MediaIoBaseDownload


def authenticate_drive():
    scopes = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file('/content/drive/MyDrive/Credentials.json',
                                                                        scopes=scopes)
    drive_service = build('drive', 'v3', credentials=credentials)
    return drive_service

import numpy as np

def calculate_vedba(telemetry_data):
    values = telemetry_data.split(',')

    telemetry_data_decimal = telemetry_data  # Initialize telemetry_data_decimal with the original value
    vedba_converted = ""  # Initialize vedba_converted with a default value
    signed_value = []  # Initialize signed_value with a default value
    vedba_value = 0.0  # Initialize vedba_value with a default value

    if telemetry_data == "FFFF,FFFF,FFFF":
        x, y, z = -1, -1, -1
        vedba_converted = f"({x}, {y}, {z})"  # Decimal representation using (x, y, z)
        signed_value = [x, y, z]  # Signed value directly assigned
        vedba_value = np.sqrt(x ** 2 + y ** 2 + z ** 2) / (256.0 * 10.0)  # Calculate vedba_value using original values (x, y, z)
    elif len(values) == 3:
        converted_values = []

        for value in values:
            if len(value) > 0:
                converted_value = value.strip()
                converted_values.append(converted_value)

        if len(converted_values) == 3:
            # Calculate vedba_converted, signed_value, vedba_value, and vedba_value_index using converted_values
            try:
                x, y, z = [int(value, 16) for value in converted_values]
                vedba_converted = f"({x}, {y}, {z})"  # Updated: Decimal representation using (x, y, z)
                signed_value = [(x - 2**16) if x >= 2**15 else (x - 2**12) if x >= 2**11 else (x - 2**8) if x >= 2**7 else x,
                                (y - 2**16) if y >= 2**15 else (y - 2**12) if y >= 2**11 else (y - 2**8) if y >= 2**7 else y,
                                (z - 2**16) if z >= 2**15 else (z - 2**12) if z >= 2**11 else (z - 2**8) if z >= 2**7 else z]

                vedba_value = np.sqrt(signed_value[0] ** 2 + signed_value[1] ** 2 + signed_value[2] ** 2) / (256.0 * 10.0)
            except ValueError:
                return None, None, None, None, None  # Skip invalid telemetry data

    # Encode vedba_value as one-hot encoding
    vedba_value_encoded = [0, 0, 0, 0, 0]
    vedba_value_encoded[int(min(30.0 * vedba_value, 4))] = 1

    return telemetry_data_decimal, vedba_converted, signed_value, vedba_value, vedba_value_encoded


def download_file_data(drive_service, file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    file_data = fh.getvalue().decode('utf-8')
    return file_data

def read_all_csv_from_drive(input_folder_id):
    drive_service = authenticate_drive()

    # Get the list of files in the TelemetryData folder
    file_list = drive_service.files().list(q=f"'{input_folder_id}' in parents and mimeType='text/csv'",
                                           fields="files(id,name)").execute()
    files = file_list.get('files', [])

    filtered_data = {}

    for file in files:
        # Download each CSV file
        csv_file_data = download_file_data(drive_service, file['id'])
        if csv_file_data:
            process_csv_data(csv_file_data, filtered_data, telemetry_data_column="telemetry_data")

    return filtered_data


def process_csv_data(csv_file_data, filtered_data, telemetry_data_column):
    csv_data = io.StringIO(csv_file_data)
    reader = csv.DictReader(csv_data)

    for row in reader:
        animal_id = row['animal_id']
        telemetry_data = row[telemetry_data_column]
        telemetry_data, vedba_converted, signed_value, vedba_value, vedba_value_encoded = calculate_vedba(telemetry_data)

        if telemetry_data and vedba_converted and signed_value and vedba_value and vedba_value_encoded:
            row[telemetry_data_column] = telemetry_data
            row['vedba_converted'] = vedba_converted
            row['signed_value'] = signed_value
            row['vedba_value'] = str(vedba_value)
            row['vedba_value_encoded'] = str(vedba_value_encoded)

            if animal_id not in filtered_data:
                filtered_data[animal_id] = []

            filtered_data[animal_id].append(list(row.values()))


def save_filtered_data(filtered_data, output_folder_id, column_headers):
    drive_service = authenticate_drive()

    for animal_id, data_rows in filtered_data.items():
        csv_file_name = f"{animal_id}.csv"
        file_metadata = {
            'name': csv_file_name,
            'mimeType': 'text/csv',
            'parents': [output_folder_id]
        }

        # Insert column headers as the first row
        data_rows.insert(0, column_headers)

        # Sort the data rows based on the "id" column in ascending order, skipping the first row
        data_rows[1:] = sorted(data_rows[1:], key=lambda row: int(row[0]))

        csv_data = io.StringIO()
        csv_writer = csv.writer(csv_data)
        csv_writer.writerows(data_rows)

        media = MediaIoBaseUpload(csv_data, mimetype='text/csv')

        try:
            drive_service.files().create(body=file_metadata, media_body=media).execute()
            print(f"CSV data for animal ID {animal_id}.csv is stored.")
        except Exception as e:
            print(f"Error storing CSV data for animal ID {animal_id}.csv: {str(e)}")


def main():
    #input_folder_id = '1H409R0TBO1-GFoPrd8IJp3t1VPDHEs_s'
    #TelemetryData- Folder
    input_folder_id = '1XQQQargysKZGIwu4RXHsh3f5NHwJf1le'

    #AnimalData -Folder
    #output_folder_id = '1miIM_pP7WgDKlNh-mb6VuN17BmLTwJ9M'
    output_folder_id = '12haC4wXejlezrFwqY-PcgXBkqWDsRodp'


    drive_service = authenticate_drive()

    # Column headers for the output CSV files
    column_headers = ['id', 'user_id', 'animal_id', 'telemetry_data', 'mac_id', 'timestamp', 'vedba_converted','signed_value','vedba_value', 'vedba_value_encoded']

    # Read all CSV files from the input folder
    filtered_data = read_all_csv_from_drive(input_folder_id)

    # Save the filtered data to the output folder
    save_filtered_data(filtered_data, output_folder_id, column_headers)

    print("Done Run")


if __name__ == '__main__':
    main()




