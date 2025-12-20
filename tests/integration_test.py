import requests
import pytest
import psycopg2

def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("API_KEY")
    channel_handle = airflow_variable("CHANNEL_HANDLE")

    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forUsername={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.exceptions.RequestException as e:
        pytest.fail(f"Request to YouTube API failed: {e}") 

