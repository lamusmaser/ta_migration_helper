import os
import re
import yt_dlp
from home.src.es.connect import ElasticWrap, IndexPaginate

rx = '[0-9]{8}_[a-zA-Z0-9_-]{11}_*.*'

# Function to extract video IDs from filenames
def extract_video_id(filename):
    match = re.match(r"(\d{8})_([a-zA-Z0-9_-]{11})_", filename)
    if match:
        return match.group(2)
    return None

# Function to get channel ID using yt-dlp
def get_channel_id(video_id):
    ydl_opts = {'quiet': True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
        return info.get('channel_id')

# Function to retrieve video IDs from Elasticsearch
def get_video_ids_from_es():
    res = IndexPaginate('ta_video',{}).get_results()
    video_ids = {}
    for hit in res:
        video_ids[hit['youtube_id']] = {"media_url": hit['media_url']}
        if hit.get('subtitles'):
            video_ids[hit['youtube_id']]['subtitles'] = []
            for sub in hit['subtitles']:
                video_ids[hit['youtube_id']]['subtitles'].append({sub['lang']: sub['media_url']})
    return video_ids

def check_video_id_from_es(video_id):
    res = ElasticWrap("ta_video/_search").get(data={"query": {"match":{"_id": video_id}}})
    if res[1] == 200:
        res = res[0]
    video_ids = {}
    for hit in res['hits']['hits']:
        video_ids[hit['_id']] = {"media_url": hit['_source']['media_url']}
        if hit['_source'].get('subtitles'):
            video_ids[hit['_id']]['subtitles'] = []
            for sub in hit['_source']['subtitles']:
                video_ids[hit['_id']]['subtitles'].append({sub['lang']: sub['media_url']})
    return video_ids

# Walk through the /youtube directory
video_files = {}
for root, dirs, files in os.walk('/youtube'):
    for filename in files:
        match = re.search(rx, filename, re.IGNORECASE)
        if match:
            print(f"Matching file: {filename}")
            video_id = extract_video_id(filename)
            if video_id:
                original_location = os.path.join(root, filename)
                channel_id = get_channel_id(video_id)
                expected_location = f"/youtube/{channel_id}/{video_id}.{os.path.splitext(filename)[-1]}"
                video_files[video_id] = {'original_location': original_location, 'expected_location': expected_location}

# Get video IDs from Elasticsearch
es_video_ids = get_video_ids_from_es()

# Create dictionaries for file system and Elasticsearch video IDs
fs_video_ids = set(video_files.keys())
es_video_ids = set(es_video_ids)

# Determine differences
videos_in_fs_not_in_es = fs_video_ids - es_video_ids
videos_in_es_not_in_fs = es_video_ids - fs_video_ids
videos_in_both = fs_video_ids.intersection(es_video_ids)

# Print the results
print("Videos in the filesystem but not in Elasticsearch:")
for video_id in videos_in_fs_not_in_es:
    if check_video_id_from_es(video_id):
        print(f"Video ID found in Elasticsearch with secondary search.")
    else:
        print(f"Video ID: {video_id}, Original Location: {video_files[video_id]['original_location']}")

print("\nVideos in Elasticsearch but not in the filesystem:")
for video_id in videos_in_es_not_in_fs:
    print(f"Video ID: {video_id}")

print("\nVideos correctly assigned to both:")
for video_id in videos_in_both:
    print(f"Video ID: {video_id}, Original Location: {video_files[video_id]['original_location']}, Expected Location: {video_files[video_id]['expected_location']}")

