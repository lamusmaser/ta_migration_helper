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
def get_channel_id(video_id, use_ytdlp, ytdlp_sleep):
    if use_ytdlp:
        ydl_opts = {'quiet': True, 'logger': FakeLogger()}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                time.sleep(ytdlp_sleep)
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
                return info.get('channel_id')
            except yt_dlp.utils.DownloadError as e:
                try:
                    return check_channel_id_from_es(video_id)
                except:
                    print(f"Failed to find video ID from YouTube or ElasticSearch for {video_id}. YouTube download error: {e}")
                    return None
    else:
        try:
            return check_channel_id_from_es(video_id)
        except:
            e = "USE_YTDLP set to False. YouTube Download Error does not exist."
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

def pull_video_from_es(video_id):
    res = ElasticWrap("ta_video/_search").get(data={"query": {"match":{"_id": video_id}}})
    if res[1] == 200:
        res = res[0]
    video_ids = {}
    for hit in res['hits']['hits']:
        video_ids[hit['_id']] = {}
        video_ids[hit['_id']]['channel_id'] = hit['_source']['channel']['channel_id']
        video_ids[hit['_id']]['media_url'] = hit['_source']['media_url']
        if hit['_source'].get('subtitles'):
            video_ids[hit['_id']]['subs'] = []
            for sub in hit['_source']['subtitles']:
                video_ids[hit['_id']]['subs'].append({sub['lang']: sub['media_url']})
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
    final_list = [nm for ps in video_ids for nm in video_list if ps in nm]
    return final_list


def review_filesystem(dir, use_ytdlp, ytdlp_sleep):
    # Walk through the /youtube directory
    print("Calculating number of files to process...")
    file_count = sum(len(files) for _, _, files in os.walk(dir))
    video_files = {}
    all_files = []
    current_count = 0

    print("Processing video files...")
    for root, _, files in os.walk(dir):
        for filename in files:
            current_count += 1
            all_files.append(os.path.join(root,filename))
            match = re.search(rx, filename, re.IGNORECASE)
            if match:
                print(f"[{current_count}/{file_count}] Matching file: {filename}")
                video_id = extract_video_id(filename)
                if video_id:
                    original_location = os.path.join(root, filename)
                    if video_files.get(video_id):
                        channel_id = video_files[video_id][0]['channel_id']
                    else:
                        channel_id = get_channel_id(video_id, use_ytdlp, ytdlp_sleep)
                    if channel_id:
                        expected_location = f"{dir}{channel_id}/{video_id}{os.path.splitext(filename)[-1]}"
                        if not video_files.get(video_id):
                            video_files[video_id] = []
                        vid_type = None
                        if os.path.splitext(filename)[-1] in ['.mp4']:
                            vid_type = 'video'
                        elif os.path.splitext(filename)[-1] in ['.vtt']:
                            vid_type = 'subtitle'
                        else:
                            vid_type = 'other'
                        video_files[video_id].append({'channel_id': channel_id, 'type': vid_type, 'original_location': original_location, 'expected_location': expected_location})

def compare_es_filesystem(fs_video_ids, es_video_ids, video_files, all_files):
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
        pull = []
        res = pull_video_from_es(video_id)
        for vid_id in res.keys():
            pull.append({
                'channel_id': res[vid_id]['channel_id'],
                'type': 'video',
                'original_location': 'NOT_AVAILABLE',
                'expected_location': f"/youtube/{res[vid_id]['media_url']}"
            })
            if res[vid_id].get('subs'):
                for sub in res[vid_id]['subs']:
                    pull.append({
                        'channel_id': res[vid_id]['channel_id'],
                        'type': 'subtitle',
                        'original_location': 'NOT_AVAILABLE',
                        'expected_location': f"/youtube/{res[vid_id]['sub'][sub]}"
                    })
        results["InESNotFS"][video_id]["details"] = pull
    results["InESInFS"] = {}
    for video_id in videos_in_both:
        results["InESInFS"][video_id] = {}
        results["InESInFS"][video_id]["secondary_result"] = "Not Required - Present In Both"
        results["InESInFS"][video_id]["details"] = video_files[video_id]
    print(json.dumps(results))
    return results

def main():
    default_source = "/youtube"
    default_use_ytdlp = True
    default_ytdlp_sleep = 3
    default_perform_migration = False
    source_dir = os.getenv("SOURCE_DIR", default_source)
    use_ytdlp = str(os.getenv("USE_YTDLP", default_use_ytdlp)).lower() in ("true", 1, "t")
    ytdlp_sleep = int(os.getenv("YTDLP_SLEEP", default_ytdlp_sleep))
    perform_migration = str(os.getenv("PERFORM_MIGRATION", default_perform_migration)).lower() in ("true", 1, "t")
    if not os.path.exists(source_dir):
        print(f"The directory `{source_dir}` does not exist. Exiting.")
        return 1
    video_files, all_files = review_filesystem(source_dir, use_ytdlp, ytdlp_sleep)
    # Get video IDs from Elasticsearch
    es_video_ids = get_video_ids_from_es()
    # Create dictionaries for file system and Elasticsearch video IDs
    fs_video_ids = set(video_files.keys())
    es_video_ids = set(es_video_ids)

    # Determine differences
    diffs = compare_es_filesystem(fs_video_ids, es_video_ids, video_files, all_files)
    if perform_migration:
        print("This is a destructive action and could cause loss of data if interrupted. Giving ten seconds before initiating migration action...")
        time.sleep(10)

if __name__ == "main":
    print("Starting script...")
    main()
    print("Script finished. Exiting.")