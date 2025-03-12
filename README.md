# Spotify-pipeline-GCP
End-to-end Spotify data pipeline using GCP.  spotify-to-gcs.py pulls data from "Country Beach Party 2025" playlist and drops CSV file daily to GCS storage bucket.  Script is scheduled with Airflow on GCP Cloud Composer.  df_trigger is a GCP Cloud Function that is triggered on file upload to GCS bucket and loads the data to BigQuery.  I then connected the data to Tableau and made some simple visualizations.

![image](https://github.com/user-attachments/assets/32449279-e28d-48ef-94cf-5595a31ec093)

![image](https://github.com/user-attachments/assets/90a066cd-55d1-4ab0-95e2-cc4eaca6312c)

![image](https://github.com/user-attachments/assets/0dda30bb-213c-4994-aeaf-c0924ff5a2f2)

![image](https://github.com/user-attachments/assets/a3d3a21b-cdbf-49bb-9208-e736a129ea79)

![image](https://github.com/user-attachments/assets/4d10c876-401d-4c6a-bcef-984b28574f04)


