import argparse
import json
import mimetypes
import os
import re
import shutil
import stat
import string
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

def parse_args():
    default_source = "/youtube"
    default_use_ytdlp = True
    default_ytdlp_sleep = 3
    default_perform_migration = False
    default_debug = False
    default_dry_run = False
    parser = argparse.ArgumentParser(description="TA Migration Helper Script")
    # Optional arguments
    parser.add_argument(
        '-d', '--SOURCE_DIR',
        default=default_source,
        help="The source directory that will be searched for videos that need to be migrated."
    )
    parser.add_argument(
        '-Y', '--USE_YTDLP',
        default=default_use_ytdlp,
        action='store_false',
        help="Disable calls to YouTube via yt-dlp. If set, it will only search ElasticSearch."
    )
    parser.add_argument(
        '-s', '--YTDLP_SLEEP',
        type=int,
        default=default_ytdlp_sleep,
        help="Number of seconds to wait between each call to YouTube when using yt-dlp. This value is not used if USE_YTDLP is set to False."
    )
    parser.add_argument(
        '-M', '--PERFORM_MIGRATION',
        default=default_perform_migration,
        action='store_true',
        help="If set to True, this will attempt to migrate all files. If False, it will perform a review of what files need to be migrated and why."
    )
    parser.add_argument(
        '-B', '--DEBUG',
        default=default_debug,
        action='store_true',
        help="If set to True, this will show debugging outputs."
    )
    parser.add_argument(
        '-r', '--DRY_RUN',
        default=default_dry_run,
        action='store_true',
        help="If set to True and PERFORM_MIGRATION is True, then it will only show what it expects to change. All details are preceeded with a DRY_RUN statement."
    )
    parser.add_argument(
        '-g', '--GUESS_TYPES',
        default=default_dry_run,
        action='store_true',
        help="If set to True, will attempt to guess the type of the files by looking at the file itself. Decreases chances of false positives based on file extension, but does access the file and can slow down the analysis."
    )
    global args
    args = parser.parse_args()
    if args.DEBUG:
        dprint("Arguments provided:")
        for arg in vars(args):
            dprint(f"\t{arg}: {getattr(args, arg)}")


def dprint(value, **kwargs):
    if args.DEBUG:
        print(f"DEBUG:\t{value}", **kwargs)

# Function to extract video IDs from filenames
def extract_video_id(filename):
    match = re.match(r"(\d{8})_([a-zA-Z0-9_-]{11})_", filename)
    if match:
        return match.group(2)
    return None

# Function to get channel ID using yt-dlp
def get_channel_id(video_id):
    if args.USE_YTDLP:
        ydl_opts = {'quiet': True, 'logger': FakeLogger()}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                time.sleep(args.YTDLP_SLEEP)
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
                dprint(f"Channel extracted from YTDL: {info.get('channel_id')}")
                return info.get('channel_id')
            except yt_dlp.utils.DownloadError as e:
                try:
                    return check_channel_id_from_es(video_id)
                except:
                    print(f"Failed to find video ID from YouTube or ElasticSearch for {video_id}. YouTube download error: {e}")
                    return None
    else:
        try:
            es_channel_id = check_channel_id_from_es(video_id)
            if es_channel_id:
                return es_channel_id
            return None 
        except:
            e = "USE_YTDLP set to False. YouTube Download Error does not exist."
            print(f"Failed to find video ID from YouTube or ElasticSearch for {video_id}. YouTube download error: {e}")
            return None


# Function to retrieve video IDs from Elasticsearch
def get_video_ids_from_es():
    print("Pulling video IDs from ElasticSearch...")
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
    dprint(f"Scanning through previously iterated filesystem list for the following video(s): {video_ids}")
    final_list = [nm for ps in video_ids for nm in video_list if ps in nm]
    return final_list


def review_filesystem(dir):
    # Walk through the /youtube directory
    print("Calculating number of files to process...")
    file_count = sum(len(files) for _, _, files in os.walk(dir))
    dprint(f"Total files found: {file_count}")
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
                video_id = extract_video_id(filename)
                print(f"[{current_count}/{file_count}] Matching file: {filename} | Extracted Video ID: {video_id}")
                if video_id:
                    original_location = os.path.join(root, filename)
                    if video_files.get(video_id):
                        channel_id = video_files[video_id][0]['channel_id']
                    elif os.path.exists(os.path.join(root,"channel.id")):
                        with open(os.path.join(root,"channel.id"), 'r') as channel_file:
                            for line in channel_file.readlines():
                                if len(line) > 0:
                                    channel_id = line.strip()
                    else:
                        channel_id = get_channel_id(video_id)
                    if channel_id:
                        expected_location = os.path.join(os.path.join(dir, channel_id),f"{video_id}{os.path.splitext(filename)[-1]}")
                        if not video_files.get(video_id):
                            video_files[video_id] = []
                        vid_type = None
                        lang = None
                        if args.GUESS_TYPES:
                            try:
                                file_mimetype = mimetypes.guess_type(original_location)[0]
                                dprint("Detected mimetype: {file_mimetype}")
                                lines = []
                                if file_mimetype == None:
                                    try:
                                        with open(original_location, 'r') as f:
                                            lines = f.readlines()
                                    except Exception as e:
                                        print(f"An error occurred while attempting to determine filetype for {filename}: {e}")
                                        vid_type = 'other'
                                    dprint("File first line [Expect `WEBVTT`]: {lines[0]}")
                                    dprint("File first line [Expect `Language`]: {lines[2]}")
                                    if "WEBVTT" in lines[0]:
                                        vid_type = 'subtitle'
                                        expected_location = os.path.join(os.path.join(dir, channel_id),f"{video_id}{os.path.splitext(filename)[-1]}")
                                        for line in lines:
                                            if "Language: " in line:
                                                lang = line.strip().split()[-1].strip().lower()
                                                expected_location = os.path.join(os.path.join(dir, channel_id),f"{video_id}.{lang}{os.path.splitext(filename)[-1]}")
                                    else:
                                        vid_type = 'other'
                                elif "video" in file_mimetype:
                                    vid_type = 'video'
                                else:
                                    vid_type = 'other'
                            except TypeError as e:
                                print(f"An error occurred while attempting to guess the filetype for {filename}: {e}")
                                vid_type = 'other'
                        else:
                            if os.path.splitext(filename)[-1] in ['.mp4']:
                                vid_type = 'video'
                            elif os.path.splitext(filename)[-1] in ['.vtt']:
                                vid_type = 'subtitle'
                                lang = os.path.splitext(os.path.splitext(filename)[0])[-1].translate(str.maketrans('', '', string.punctuation))
                                expected_location = os.path.join(os.path.join(dir, channel_id),f"{video_id}.{lang}{os.path.splitext(filename)[-1]}")
                            else:
                                vid_type = 'other'
                        det = {'channel_id': channel_id, 'type': vid_type, 'original_location': original_location, 'expected_location': expected_location}
                        dprint("File is of type: {vid_type}")
                        if vid_type == 'subtitle':
                            dprint("Subtitle language: {lang}")
                            det['lang'] = lang
                        video_files[video_id].append(det)
                    else:
                        print(f"Could not extract channel ID for `{filename}`.")
                else:
                    print(f"Could not extract video ID for `{filename}`.")
    dprint(f"All video files: {video_files}.")
    dprint(f"All files in filesystem: {all_files}")
    return video_files, all_files

def compare_es_filesystem(video_files, all_files, source):
    es_video_ids = get_video_ids_from_es()
    fs_video_ids_set = set(video_files.keys())
    es_video_ids_set = set(es_video_ids.keys())

    print("Comparing Filesystem and ElasticSearch results.")
    videos_in_fs_not_in_es = fs_video_ids_set - es_video_ids_set
    videos_in_es_not_in_fs = es_video_ids_set - fs_video_ids_set
    videos_in_both = fs_video_ids_set.intersection(es_video_ids_set)

    dprint(f"Filesystem videos: {fs_video_ids_set}")
    dprint(f"ElasticSearch videos: {es_video_ids_set}")
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
                'original_location': os.path.join(source, res[vid_id]['media_url']),
                'expected_location': os.path.join(os.path.join(source, res[vid_id]['channel_id']), f"{vid_id}.mp4")
            })
            if res[vid_id].get('subs'):
                for sub in res[vid_id]['subs']:
                    for lang, media_url in sub.items():
                        pull.append({
                            'channel_id': res[vid_id]['channel_id'],
                            'type': 'subtitle',
                            'original_location': os.path.join(source, media_url),
                            'expected_location': os.path.join(os.path.join(source, res[vid_id]['channel_id']), f"{vid_id}.{lang}.vtt"),
                            'lang': lang
                        })
        results["InESNotFS"][video_id]["details"] = pull
    results["InESInFS"] = {}
    for video_id in videos_in_both:
        results["InESInFS"][video_id] = {}
        results["InESInFS"][video_id]["secondary_result"] = "Not Required - Present In Both"
        results["InESInFS"][video_id]["details"] = video_files[video_id]
    print("-"*150)
    print(json.dumps(results))
    print("-"*150)
    return results

def prep_directory(root, source, channel_id):
    dest_directory_path = os.path.join(root, channel_id)
    try:
        os.makedirs(dest_directory_path, exist_ok=True)
        source_stat = os.stat(source)
        uid = source_stat.st_uid
        gid = source_stat.st_gid
        permissions = stat.S_IMODE(source_stat.st_mode)
        os.chown(dest_directory_path, uid, gid)
        os.chmod(dest_directory_path, permissions)
    except Exception as e:
        print(f"An error occurred during the directory prep function: {e}")

def update_es_for_item(id, nmu, vid_type, lang):
    new_media_url = nmu
    if vid_type == 'subtitle':
        res = ElasticWrap("ta_video/_search").get(data={"query": {"match":{"_id": id}}})
        if res[1] == 200:
            res = res[0]
        subtitles = res['hits']['hits'][0]['_source']['subtitles']
        for i, sub in enumerate(subtitles):
            if sub['lang'] == lang:
                subtitles[i]['media_url'] = new_media_url
        source = {"doc": {"subtitles": subtitles}}
    else:
        source = {"doc": {"media_url": new_media_url}}
    if args.DRY_RUN:
        print(f"DRY_RUN:\tUpdating ElasticSearch values for {id}'s{' ' + lang + ' ' if lang else ' '}{vid_type} | {source}")
    else:
        print(f"Updating ElasticSearch values for {id}'s{' ' + lang + ' ' if lang else ' '}{vid_type}.")
        res = ElasticWrap(f"ta_video/_update/{id}").post(data = source)
        try:
            if res[1] == 200 and res[0]['_shards']['total'] == res[0]['_shards']['successful']:
                print(f"ElasticSearch was updated successfully.")
            else:
                print(f"ElasticSearch was not updated successfully.")
        except Exception as e:
            print(f"Exception occurred during update of ElasticSearch: {e}")

def migration(root, id, source_file, dest_file_obj):
    if args.DRY_RUN:
        print(f"DRY_RUN:\tDirectory would be created or confirmed as created here: {os.path.join(root, dest_file_obj['channel_id'])}")
        print(f"DRY_RUN:\tMoving file `{source_file}` to `{dest_file_obj['expected_location']}`.")
        nmu = '/'.join(dest_file_obj["expected_location"].split('/')[2:])
        update_es_for_item(id, nmu, dest_file_obj['type'], dest_file_obj['lang'] if dest_file_obj.get('lang') else None)
    else:
        prep_directory(root, os.path.dirname(source_file), dest_file_obj["channel_id"])
        print(f"Moving file `{source_file}` to `{dest_file_obj['expected_location']}`.")
        shutil.move(source_file, dest_file_obj["expected_location"])
        nmu = '/'.join(dest_file_obj["expected_location"].split('/')[2:])
        update_es_for_item(id, nmu, dest_file_obj['type'], dest_file_obj['lang'] if dest_file_obj.get('lang') else None)

def migrate_files(diffs, all_files, root):
    flag_filesystem_rescan = False
    flag_filesystem_rescan_list = []
    if diffs.get("InESNotFS"):
        for video in diffs["InESNotFS"].keys():
            if diffs["InESNotFS"][video].get("secondary_result") and diffs["InESNotFS"][video]["secondary_result"] == "Secondary Search Found Result":
                print(f"At least 1 file for {video} was detected on your filesystem. Attempting to migrate those files to the new naming scheme.")
                files_fs = check_filesystem_for_video_ids(all_files, [video])
                for file_fs in files_fs:
                    file_fs_type = None
                    if args.GUESS_TYPES:
                        try:
                            if mimetypes.guess_type(file_fs)[0] == None:
                                with open(file_fs, 'r') as f:
                                    firstline = f.readline().strip('\n')
                                if "WEBVTT" in firstline:
                                    vid_type = 'subtitle'
                                else:
                                    vid_type = 'other'
                            elif "video" in mimetypes.guess_type(file_fs)[0]:
                                vid_type = 'video'
                            else:
                                vid_type = 'other'
                        except TypeError as e:
                            vid_type = 'other'
                    else:
                        if os.path.splitext(file_fs)[-1] in ['.mp4']:
                                vid_type = 'video'
                        elif os.path.splitext(file_fs)[-1] in ['.vtt']:
                            vid_type = 'subtitle'
                        else:
                            vid_type = 'other'
                    for file_es in diffs["InESNotFS"][video]["details"]:
                        try:
                            if file_fs != file_es["expected_location"] and file_es["original_location"] != file_es["expected_location"] and file_fs_type == file_es["type"]:
                                migration(root, video, file_fs, file_es)
                            else:
                                print(f"No migration necessary for `{file_fs}`. File is already using the expected naming format.")
                        except Exception as e:
                            print(f"An issue occurred during the migration of files for ID {video}. Please review the exception: {e}")
                            continue
            elif diffs["InESNotFS"][video].get("secondary_result") and diffs["InESNotFS"][video]["secondary_result"] == "Not Found In Filesystem":
                print(f"Files for {video} do not exist in filesystem. A filesystem rescan will remove video {video} from your TubeArchivist instance. If the videos are present elsewhere in your filesystem, please add them to `{root}`.")
                flag_filesystem_rescan = True
                flag_filesystem_rescan_list.append(video)
                continue
            else:
                print(f"Files for {video} did not meet appropriate criteria. Please review the JSON output and determine why this occurred.")
    if diffs.get("InESInFS"):
        for video in diffs["InESInFS"].keys():
            print(f"At least 1 file for {video} was detected on your filesystem and in ElasticSearch. Attempting to migrate to the new naming scheme.")
            for file in diffs["InESInFS"][video]["details"]:
                try:
                    if file["original_location"] != file["expected_location"]:
                        migration(root, video, file['original_location'], file)
                    else:
                        print(f"No migration necessary for `{file['original_location']}`. File is already using the expected naming format.")
                except Exception as e:
                    print(f"An issue occurred during the migration of files for ID {video}. Please review the exception: {e}")
    if diffs.get("InFSNotES"):
        if flag_filesystem_rescan:
            print(f"A filesystem rescan is expected to be performed to add these videos to your TubeArchivist instance. It was noted that there are some videos in ElasticSearch that do not exist in your filesystem. Please retain those records to download, import, or migrate those videos again in the future.")
            print(flag_filesystem_rescan_list)
            print("No action taken at this time. Please perform the action from within the TubeArchivist GUI.")

def main():
    parse_args()
    source_dir = args.SOURCE_DIR
    if not os.path.exists(source_dir):
        print(f"The directory `{source_dir}` does not exist. Exiting.")
        return 1
    video_files, all_files = review_filesystem(source_dir)

    diffs = compare_es_filesystem(video_files, all_files, source_dir)
    if args.PERFORM_MIGRATION:
        if args.DRY_RUN:
            print("This is a dry-run of the migration action and should not perform any filesystem activities. Please review all DRY_RUN outputs before running without this flag.")
        else:
            print("This is a destructive action and could cause loss of data if interrupted. Giving ten seconds before initiating migration action...")
            time.sleep(10)
        print("Starting the migration process. PLEASE DO NOT INTERRUPT THIS PROCESS.")
        migrate_files(diffs, all_files, source_dir)
        print("Ending the migration process.")

if __name__ == "__main__":
    print("Starting script...")
    main()
    print("Script finished. Exiting.")