from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import base64
import pandas as pd
import os
from google.cloud import storage

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

# DAG Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# GCP & Storage Details
PROJECT_ID = "FA24-I535-tyleluca-SpotifyData"
GCS_BUCKET = "tyleluca-spotify-bucket"
FILE_NAME = f"todays_top_hits_{datetime.now().strftime('%Y%m%d')}.csv"

# Function to Get Spotify Access Token
def get_access_token():
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(url, headers=headers, data={"grant_type": "client_credentials"})
    token = response.json().get("access_token")
    return token

# Returning list of dictionaries that specify data about each track in the playlist Country Beach Party 
# (only returning the first artist listed for each track as some tracks have multiple artists listed)

def get_country_beach_party():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Playlist ID for "Country Beach Party 2025" 
    playlist_id = "0A7njjO6LMimV1ce46gLJw"
    tracks_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    all_tracks = []

    # Fetch all tracks from the playlist, paginating if necessary
    while tracks_url:
        response = requests.get(tracks_url, headers=headers)
        data = response.json()
        all_tracks.extend(data.get("items", []))
        # API provides the next URL if there are more tracks
        tracks_url = data.get("next")

    # Extract track details from the full data
    track_list = [{
        "track_id": item["track"]["id"],
        "track_name": item["track"]["name"],
        "artist_id": item["track"]["artists"][0]["id"],
        "artist_name": item["track"]["artists"][0]["name"],
        "album_id": item["track"]["album"]["id"],
        "album_name": item["track"]["album"]["name"],
        "added_at": item["added_at"]
    } for item in all_tracks]

    # Convert to a DataFrame for easier export
    return pd.DataFrame(track_list)

# Function to Upload Data to GCS
def upload_to_gcs():
    tracks_df = get_country_beach_party()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(FILE_NAME)
    # Convert to CSV and upload
    blob.upload_from_string(tracks_df.to_csv(index=False, header=True), content_type="text/csv")

# # Defining DAG
with DAG(
    dag_id="spotify_to_gcs_airflow",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_fetch_and_upload = PythonOperator(
        task_id="fetch_spotify_and_upload",
        python_callable=upload_to_gcs,
    )

    task_fetch_and_upload