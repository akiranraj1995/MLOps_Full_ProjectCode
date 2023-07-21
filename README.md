# MLOps_Full_ProjectCode

MLOps Project - This project is consisting of 3 sections -->  1.[TelemetryData]-Fetching Raw data from the api having telemetrydata records,2.[AnimalData]- (animal_id).csv is generated for each animalid from telemetrydata and processing and calculation are done to find the vedba_value,vedba_encoded_value (One-Hot_encoding) 3.[PredictedData] the latest animalid is used for making prediction using Multinomial Hidden Markov Model to predict the Hidden state of that animal id.

Storage used: Google Drive api and storage,
IDE used: Google Colab
Pre requisite: Service account for Google Drive is necessary for this project i renamed mine as "Credentials.json". Create Main folder where you want to store the TelemetryData,AnimalData,PredictedData . I have used in the above code [ML_Project_MHMM] as my main folder and inside it all the sub folder for storing the processed csvs [TelemetryData,AnimalData,PredictedData] .  

For example :
REPLACE THE PATH WITH YOUR MAIN FOLDER {ML_Project_MHMM} & INSIDE IT THE SUB FOLDER {TelemetryData},{AnimalData},{PredictedData} TO STORE THE CSV
csv_file_path = os.path.join('/content/drive/MyDrive/ML_Project_MHMM/TelemetryData', csv_file_name)

