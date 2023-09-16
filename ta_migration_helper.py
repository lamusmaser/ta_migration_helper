import json
import os
import re
import time

import yt_dlp
from home.src.es.connect import ElasticWrap, IndexPaginate

rx = '[0-9]{8}_[a-zA-Z0-9_-]{11}_*.*'

class FakeLogger(object):
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass

# Function to extract video IDs from filenames
def extract_video_id(filename):
    match = re.match(r"(\d{8})_([a-zA-Z0-9_-]{11})_", filename)
    if match:
        return match.group(2)
    return None

# Function to get channel ID using yt-dlp
def get_channel_id(video_id):
    ydl_opts = {'quiet': True, 'logger': FakeLogger()}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            time.sleep(3)
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            return info.get('channel_id')
        except yt_dlp.utils.DownloadError as e:
            try:
                return check_channel_id_from_es(video_id)
            except:
                print(f"Failed to find video ID from YouTube or ElasticSearch for {video_id}. YouTube download error: {e}")
                return None


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

def check_channel_id_from_es(video_id):
    res = ElasticWrap("ta_video/_search").get(data={"query": {"match":{"_id": video_id}}})
    if res[1] == 200:
        res = res[0]
    channel_id = None
    for hit in res['hits']['hits']:
        channel_id = hit['_source']['channel']['channel_id']
    return channel_id

def check_filesystem_for_video_ids(video_list, video_ids):
    final_list = [nm for ps in video_id for nm in video_list if ps in nm]
    return final_list

# Walk through the /youtube directory
video_files = {}
all_files = []
for root, dirs, files in os.walk('/youtube'):
    for filename in files:
        all_files.append(os.path.join(root,filename))
        match = re.search(rx, filename, re.IGNORECASE)
        if match:
            print(f"Matching file: {filename}")
            video_id = extract_video_id(filename)
            if video_id:
                original_location = os.path.join(root, filename)
                channel_id = get_channel_id(video_id)
                if channel_id:
                    expected_location = f"/youtube/{channel_id}/{video_id}.{os.path.splitext(filename)[-1]}"
                    if not video_files.get(video_id):
                        video_files[video_id] = []
                    vid_type = None
                    if os.path.splitext(filename)[-1] in ['mp4']:
                        vid_type = 'video'
                    elif os.path.splitext(filename)[-1] in ['vtt']:
                        vid_type = 'subtitle'
                    else:
                        vid_type = 'other'
                    video_files[video_id].append({'type': vid_type, 'original_location': original_location, 'expected_location': expected_location})

# Get video IDs from Elasticsearch
es_video_ids = get_video_ids_from_es()

# Create dictionaries for file system and Elasticsearch video IDs
fs_video_ids = set(video_files.keys())
es_video_ids = set(es_video_ids)

# Determine differences
videos_in_fs_not_in_es = fs_video_ids - es_video_ids
videos_in_es_not_in_fs = es_video_ids - fs_video_ids
videos_in_both = fs_video_ids.intersection(es_video_ids)

results = {}

results["InFSNotES"] = {}
for video_id in videos_in_fs_not_in_es:
    results["InFSNotES"][video_id] = {}
    if check_video_id_from_es(video_id):
        results["InFSNotES"][video_id]["secondary_result"] = "Secondary Search Found Result"
    else:
        results["InFSNotES"][video_id]["secondary_result"] = "Not Found In ElasticSearch"
    results["InFSNotES"][video_id]["details"] = video_files[video_id]
results["InESNotFS"] = {}
for video_id in videos_in_es_not_in_fs:
    results["InESNotFS"][video_id] = {}
    if check_filesystem_for_video_ids(all_files, [video_id]):
        results["InESNotFS"][video_id]["secondary_result"] = "Secondary Search Found Result"
    else:
        results["InESNotFS"][video_id]["secondary_result"] = "Not Found In Filesystem"
    results["InESNotFS"][video_id]["details"] = video_files[video_id]
results["InESInFS"] = {}
for video_id in videos_in_both:
    results["InESInFS"][video_id] = {}
    results["InESInFS"][video_id]["secondary_result"] = "Not Required - Present In Both"
    results["InESInFS"][video_id]["details"] = video_files[video_id]
print(json.dumps(results))