from datetime import timedelta, datetime

def parse_duration(duration_str):
    duration_str = duration_str.replace("P", "").replace("T", "")
    components = ['D', 'H', 'M', 'S']
    values = {'D':0,'H':0,'M':0,'S':0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    return timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )

def transform_data(row):
    duration_td = parse_duration(row["Duration"])

    return {
        "Video_ID": row["Video_ID"],
        "Video_Title": row["Video_Title"],
        "Upload_Date": row["Upload_Date"],
        "Duration": (datetime.min + duration_td).time(),
        "Video_Type": "Shorts" if duration_td.total_seconds() <= 60 else "Normal",
        "Video_Views": row["Video_Views"],
        "Likes_Count": row["Likes_Count"],
        "Comments_Count": row["Comments_Count"],  
    }
