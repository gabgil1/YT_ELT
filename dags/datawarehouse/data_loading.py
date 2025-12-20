import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    file_path = f"/opt/airflow/data/YT_data_{date.today()}.json"

    try:
        logger.info(f"Processing file: YT_data_{date.today()}")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            raw_json = json.load(raw_data)

        # Normalizar cada item del JSON a las claves esperadas en staging
        normalized_rows = []
        for item in raw_json:
            video_id = item.get("id")

            if not video_id:
                logger.warning("Skipping row with missing video_id")
                continue

            row = {
                "video_id": video_id,
                "title": item.get("snippet", {}).get("title"),
                "publishedAt": item.get("snippet", {}).get("publishedAt"),
                "duration": item.get("contentDetails", {}).get("duration"),
                "viewCount": item.get("statistics", {}).get("viewCount"),
                "likeCount": item.get("statistics", {}).get("likeCount"),
                "commentsCount": item.get("statistics", {}).get("commentCount"),
            }

            normalized_rows.append(row)

        return normalized_rows

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise
