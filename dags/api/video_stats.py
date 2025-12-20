import requests
import json
from datetime import date
import os

#from dotenv import load_dotenv
#load_dotenv(dotenv_path="./.env")

from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_ID = Variable.get("CHANNEL_ID")
maxResults = 50

@task
def get_playlist_Id():
    try:
        # Se usa channelId, NO username
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&id={CHANNEL_ID}&key={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        channel_items = data["items"][0]
        playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        print("Playlist ID:", playlistId)

        return playlistId

    except requests.exceptions.RequestException as e:
        raise e

@task
def get_videos_ids(playlistId):
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
    videos_ids = []
    pageToken = None

    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                videos_ids.append(video_id)

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break

        print("Videos IDs:", videos_ids)
        return videos_ids

    except requests.exceptions.RequestException as e: 
        raise e

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_list, batch_size):
        for idx in range(0, len(video_id_list), batch_size):
            yield video_id_list[idx: idx + batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            url = (
                "https://youtube.googleapis.com/youtube/v3/videos?"
                f"part=contentDetails&part=statistics&part=snippet&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_data = {
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "publishedAt": item["snippet"]["publishedAt"],
                    "viewCount": item["statistics"].get("viewCount"),
                    "likeCount": item["statistics"].get("likeCount"),
                    "commentCount": item["statistics"].get("commentCount"),
                    "duration": item["contentDetails"]["duration"]
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    # Crear carpeta data si no existe
    os.makedirs("./data", exist_ok=True)

    file_path = f"./data/YT_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

    print(f"Archivo guardado en: {file_path}")


if __name__ == "__main__":
    playlistId = get_playlist_Id()
    videos_ids = get_videos_ids(playlistId)
    video_data = extract_video_data(videos_ids)
    save_to_json(video_data)
