#3.[PREDICTEDDATA]
#PredictedData--># 3. [FINAL CODE]--> ML PREDICTION USING MULTINOMIAL HIDDEN MARKOV MODEL (Predicted_id.csv generated)

#3. Mhmm (FINAL CODE -RESOLVED BASED ON DOC RESULT PRINTED )

!pip install hmmlearn
import os
import csv
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import googleapiclient
from datetime import date
import numpy as np
#from sklearn.impute import SimpleImputer
from hmmlearn import hmm
import io
import ast

# Path to the credentials.json file
credentials_path = '/content/drive/MyDrive/Credentials.json'

# TelemetryData folder ID
telemetry_folder_id = '1XQQQargysKZGIwu4RXHsh3f5NHwJf1le'

# PredictedData- folder ID
prediction_folder_id = '1ia6utfm-I9UjIybRNuz5KtXOGQRkoKAb'

#AnimalData folder
animal_data_folder_id='12haC4wXejlezrFwqY-PcgXBkqWDsRodp'

# Function to generate Scheduled_id.csv
def generate_scheduled_id_csv(drive_service, telemetry_folder_id, parent_folder_id):
    # Get the list of CSV files in the TelemetryData folder
    results = drive_service.files().list(
        q=f"'{telemetry_folder_id}' in parents and mimeType='text/csv'",
        pageSize=1000,
        fields="nextPageToken, files(id, name)"
    ).execute()

    csv_files = results.get('files', [])

    if not csv_files:
        print('No CSV files found in the TelemetryData folder.')
        return

    # Sort CSV files by name in ascending order
    csv_files = sorted(csv_files, key=lambda x: x['name'])

    # Prepare the output DataFrame
    output_data = []

    # Read and process each CSV file
    for file in csv_files:
        file_name = file['name']
        file_id = file['id']
        print(f"Processing CSV file: {file_name}")

        # Download the CSV file
        file_path = os.path.join('/tmp', file_name)
        request = drive_service.files().get_media(fileId=file_id)
        fh = open(file_path, 'wb')
        downloader = googleapiclient.http.MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

        # Read the CSV file and extract unique animal IDs
        csv_data = pd.read_csv(file_path)
        unique_animal_ids = csv_data['animal_id'].unique()

        # Append unique animal IDs to the output data list
        output_data.append(unique_animal_ids.tolist())

        # Remove the downloaded CSV file
        os.remove(file_path)

    # Create a DataFrame from the output data
    output_df = pd.DataFrame(output_data).transpose()
    output_df.columns = [f'Chunk{str(i).zfill(2)}' for i in range(1, len(csv_files) + 1)]

    # Save the output DataFrame to Scheduled_id.csv in the Prediction_MHMM folder
    file_metadata = {
        'name': 'Scheduled_id.csv',
        'parents': [parent_folder_id]
    }
    output_df.to_csv('/tmp/Scheduled_id.csv', index=False)
    media = googleapiclient.http.MediaFileUpload('/tmp/Scheduled_id.csv')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print('Scheduled_id.csv saved successfully in the  folder!')

# Function to create and save the Schedule-date.csv file
def create_schedule_date_csv(drive_service, parent_folder_id):
    schedule_date = date.today().strftime("%Y-%m-%d")
    schedule_date_df = pd.DataFrame({'Date': [schedule_date]})
    content = schedule_date_df.to_csv(index=False)

    # Save the Schedule-date.csv file in the specified folder
    file_metadata = {
        'name': 'Schedule-date.csv',
        'parents': [parent_folder_id]
    }
    media = googleapiclient.http.MediaIoBaseUpload(io.StringIO(content), mimetype='text/csv')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print('Schedule-date.csv saved successfully.')


# Function to predict the hidden state using Multinomial HMM
def predict_hidden_state():
    # Load the credentials from the file
    credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=['https://www.googleapis.com/auth/drive'])

    # Build the Google Drive API service
    drive_service = build('drive', 'v3', credentials=credentials)

    # Read the Scheduled_id.csv file from the Prediction_MHMM folder
    scheduled_id_file_id = drive_service.files().list(
        q=f"'{prediction_folder_id}' in parents and name='Scheduled_id.csv'",
        pageSize=1,
        fields="files(id)"
    ).execute().get('files', [])[0]['id']
    scheduled_id_file = drive_service.files().get_media(fileId=scheduled_id_file_id)
    scheduled_id_df = pd.read_csv(io.BytesIO(scheduled_id_file.execute()))

    # Extract the last column values
    last_column_values = scheduled_id_df.iloc[:, -1].dropna().astype(int).tolist()

    # Create Predicted_id DataFrame with the last column values and predicted hidden states
    predicted_id_df = pd.DataFrame({'Actual Value': last_column_values, 'Predicted Hidden State': np.nan})

    # Load the corresponding animal_id CSV file for fitting the model and predicting hidden states
    for actual_value in last_column_values:
        animal_id_file_id = drive_service.files().list(
            q=f"'{animal_data_folder_id}' in parents and name='{actual_value}.csv'",
            pageSize=1,
            fields="files(id)"
        ).execute().get('files', [])[0]['id']
        animal_id_file = drive_service.files().get_media(fileId=animal_id_file_id)
        animal_id_df = pd.read_csv(io.BytesIO(animal_id_file.execute()))

        # Convert list-like strings to numpy arrays of integers
        animal_ids = animal_id_df["vedba_value_encoded"].apply(lambda x: ast.literal_eval(x)).apply(np.array).values
        animal_ids = np.vstack(animal_ids).astype(int)

        # Replace negative values with 0
        animal_ids[animal_ids < 0] = 0

        # Convert animal_ids to nonnegative integers
        animal_ids = animal_ids.astype(int)

        print(" 1. Observations:",len(animal_ids))

        # Fit the Multinomial HMM model
        n_components = 2  # Adjust the number of hidden states as needed
        model = hmm.MultinomialHMM(n_components=n_components, n_trials=1, n_iter=50, init_params='e')

        # Determine the number of unique emission values
        model.n_features = 5

         # Set the initial probabilities, emission probabilities, and transition matrix
        start_probs = np.array([0.6, 0.4])
        trans_mat = np.array([[0.8, 0.2], [0.2, 0.8]])

        emission_probs = np.array([[0.25, 0.1, 0.4, 0.25, 0.0],
                                   [0.2, 0.5, 0.1, 0.2, 0.0]])

        # Reshape the emission probabilities to match the required shape
        #emission_probs = emission_probs.reshape((n_components, n_features))
        model.emissionprob_ = emission_probs

        # Set the model parameters
        model.startprob_ = start_probs
        model.transmat_ = trans_mat
        #model.emissionprob_ = emission_probs

        print(" 2. model.startprob_ Before fitting:", model.startprob_)
        print(" 3. model.trans_mat Before fitting:", model.transmat_)
        print(" 4. model.emissionprob_ Before fitting:", model.emissionprob_)

        # Fit the model to the data
        model.fit(animal_ids)

        # Decode the hidden states
        #_, hidden_states = model.decode(animal_ids)
        logprob,received=model.decode(animal_ids)

        print(" 5. logprob:",logprob)
        print(" 6. received:",received)

        print(" 7. model.startprob_ After fitting:", model.startprob_)
        print(" 8. model.trans_mat After fitting:", model.transmat_)
        print(" 9. model.emissionprob_ After fitting:", model.emissionprob_)

        #model.predict to predict the animal_ids observation
        hidden_states = model.predict(animal_ids)

        # Predict the hidden state for the last value
        hidden_state = hidden_states[-1]

        # Update the predicted hidden state in the dataframe
        predicted_id_df.loc[predicted_id_df['Actual Value'] == actual_value, 'Predicted Hidden State'] = hidden_state

    print(" 10. Predicted_id:",predicted_id_df)

    # Save the Predicted_id.csv file in the Prediction_MHMM folder
    file_metadata = {
        'name': 'Predicted_id.csv',
        'parents': [prediction_folder_id]
    }
    predicted_id_df.to_csv('/tmp/Predicted_id.csv', index=False)
    media = googleapiclient.http.MediaFileUpload('/tmp/Predicted_id.csv')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print('Predicted_id.csv saved successfully in the folder!')


# Main function
def main():
    # Load the credentials from the file
    credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=['https://www.googleapis.com/auth/drive'])

    # Build the Google Drive API service
    drive_service = build('drive', 'v3', credentials=credentials)

    # Generate the Scheduled_id.csv file
    generate_scheduled_id_csv(drive_service, telemetry_folder_id, prediction_folder_id)

    # Create the Schedule-date.csv file
    create_schedule_date_csv(drive_service, prediction_folder_id)

    # Predict the hidden state using Multinomial HMM
    predict_hidden_state()

# Execute the main function
if __name__ == '__main__':
    main()

