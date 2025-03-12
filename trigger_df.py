import functions_framework
import google.cloud.bigquery as bigquery
import google.cloud.storage as storage
from datetime import datetime, timezone

bucket_name = "tyleluca-spotify-bucket" 
dataset_name = "spotify_data"
stage_table_name = "stage_cbp_playlist"
cbp_table_name = "cbp_playlist"
tracks_table_name = "tracks"
artists_table_name = "artists"
albums_table_name = "albums"

@functions_framework.cloud_event
def gcs_to_bq(event):

    # Extracting file details from the event
    bucket_name = event.data["bucket"]
    file_name = event.data["name"]

    if not file_name.endswith(".csv"):
        print(f"Skipping non-CSV file: {file_name}")
        return

    # Initializing BigQuery client
    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset(dataset_name)
  
    # Defining table references
    stage_table_ref = dataset_ref.table(stage_table_name)
    cbp_table_ref = dataset_ref.table(cbp_table_name)
    tracks_table_ref = dataset_ref.table(tracks_table_name)
    artists_table_ref = dataset_ref.table(artists_table_name)
    albums_table_ref = dataset_ref.table(albums_table_name)

    # Defining the GCS file path
    uri = f"gs://{bucket_name}/{file_name}"

    # Loading job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Loading data into a staging table to process
    load_job = bq_client.load_table_from_uri(uri, stage_table_ref, job_config=job_config)
    load_job.result()

    timestamp_now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    stage_data_query = f"""
        SELECT track_id, artist_id, album_id, added_at
        FROM `{dataset_name}.{stage_table_ref.table_id}`
    """
    stage_data = list(bq_client.query(stage_data_query).result())

    current_tracks_query = f"""
        SELECT track_id, removed_at
        FROM `{dataset_name}.{cbp_table_ref.table_id}`
        WHERE removed_at IS NULL
    """

    current_tracks = list(bq_client.query(current_tracks_query).result())
    current_track_ids = {row.track_id for row in current_tracks}

    removed_tracks = current_track_ids - {row.track_id for row in stage_data}

     # Updating the removed_at column for tracks that have been removed
    if removed_tracks:
        update_removed_query = f"""
            UPDATE `{dataset_name}.{cbp_table_ref.table_id}`
            SET removed_at = '{timestamp_now}'
            WHERE track_id IN ({', '.join([str(track) for track in removed_tracks])})
            AND removed_at IS NULL
        """
        bq_client.query(update_removed_query).result()

    # Inserting the new or updated tracks into cbp_playlist, tracks, artists, and albums lookup tables
    for row in stage_data:
        # Checking if track already exists in cbp_playlist (track_id exists and removed_at is NULL)
        check_duplicate_query = f"""
            SELECT 1
            FROM `{dataset_name}.{cbp_table_ref.table_id}`
            WHERE track_id = {row.track_id} AND removed_at IS NULL
            LIMIT 1
        """
        result = list(bq_client.query(check_duplicate_query).result())

        # Only inserting if the track does not already exist in the table
        if not result:
            # Inserting into cbp_playlist table
            insert_cbp_query = f"""
                INSERT INTO `{dataset_name}.{cbp_table_ref.table_id}` (track_id, artist_id, album_id, added_at, removed_at)
                VALUES ({row.track_id}, {row.artist_id}, {row.album_id}, '{row.added_at}', NULL)
            """
            bq_client.query(insert_cbp_query).result()

        # Inserting into tracks lookup table
        insert_track_query = f"""
            INSERT INTO `{dataset_name}.{tracks_table_ref.table_id}` (track_id, track_name)
            SELECT {row.track_id}, track_name
            FROM `{dataset_name}.{cbp_table_ref.table_id}`
            WHERE track_id = {row.track_id}
            AND NOT EXISTS (
                SELECT 1 FROM `{dataset_name}.{tracks_table_ref.table_id}` WHERE track_id = {row.track_id}
            )
        """
        bq_client.query(insert_track_query).result()

        # Inserting into artists lookup table
        insert_artist_query = f"""
            INSERT INTO `{dataset_name}.{artists_table_ref.table_id}` (artist_id, artist_name)
            SELECT {row.artist_id}, artist_name
            FROM `{dataset_name}.{cbp_table_ref.table_id}`
            WHERE artist_id = {row.artist_id}
            AND NOT EXISTS (
                SELECT 1 FROM `{dataset_name}.{artists_table_ref.table_id}` WHERE artist_id = {row.artist_id}
            )
        """
        bq_client.query(insert_artist_query).result()

        # Inserting into albums lookup table
        insert_album_query = f"""
            INSERT INTO `{dataset_name}.{albums_table_ref.table_id}` (album_id, album_name)
            SELECT {row.album_id}, album_name
            FROM `{dataset_name}.{cbp_table_ref.table_id}`
            WHERE album_id = {row.album_id}
            AND NOT EXISTS (
                SELECT 1 FROM `{dataset_name}.{albums_table_ref.table_id}` WHERE album_id = {row.album_id}
            )
        """
        bq_client.query(insert_album_query).result()

    return f"File {file_name} processed successfully and loaded into BigQuery."
