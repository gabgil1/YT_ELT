import requests
import json
import os
from dotenv import load_dotenv
 
load_dotenv(dotenv_path="./.env")
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
maxResults = 50


def get_playlist_Id():

    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forUsername={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        print("Playlist ID:", channel_playlistId)

        return channel_playlistId
    

    except requests.exceptions.RequestException as e:
        raise e


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


if __name__ == "__main__":
    playlistId = get_playlist_Id()
    print(get_videos_ids(playlistId))
