# TA Migration Helper [v0.3.6 to v0.4.0]
> [!NOTE]
> UNOFFICIAL HELPER SCRIPT

Migration helper for [TubeArchivist](https://github.com/tubearchivist/tubearchivist) from the legacy filenaming convention (v0.3.6 and earlier) to the current naming convention (v0.4.0 and later).

## Current functionality
1. Detects videos in filesystem that are not in ElasticSearch.
2. Detects videos that are in ElasticSearch and not in the filesystem.
3. Provides and output of what those videos are.
4. Allow automatic migration for those files that are detected.
5. Allow automatic updates to Elasticsearch.
6. Allow migration of subtitles/other files found.

## Limitations
1. This is expected to run within a running TubeArchivist container instance that is running v0.4.0 and had issues with migrating a larger library of files.
2. This is only expecting to move `.mp4` and `.vtt` files that align with TubeArchivist downloads. Will not move other files at this time.
3. If a file exists on the Filesystem in the `SOURCE_DIR` and not within ElasticSearch, you will need to run a manual [Filesystem Rescan](https://docs.tubearchivist.com/settings/actions/#rescan-filesystem) or [Manual Import](https://docs.tubearchivist.com/settings/actions/#manual-media-files-import) to pickup those files. This will be alerted if an automated migration is attempted.
4. If a file exists within ElasticSearch and not on the Filesystem in the `SOURCE_DIR`, you will need to locate the files before attempting to perform a migration. Alternatively, if you perform a manual [Filesystem Rescan](https://docs.tubearchivist.com/settings/actions/#rescan-filesystem), TubeArchivist will remove those entries for you.
5. This makes several assumptions during the runtime process. If those are incorrect or different from how TubeArchivist expects them to be performed, those specific items may not work properly and will need to be updated manually.
6. If a channel no longer exists for `yt-dlp` to find or within ElasticSearch, you will need to include a `channel.id` file within the channel's directory that includes its ID.

## Additional Arguments
> [!WARNING]
> Using the `PERFORM_MIGRATION` action is a destructive process and could cause issues with files. It is recommended to not use it unless advised or after you have reviewed an initial output of what is expected to happen.

Argument | Flag | Default | Purpose
:--- | :---: | :---: | :---
`SOURCE_DIR` | -d | `/youtube` | The source directory that will be searched for videos that need to be migrated. This can be used to specify an individual folder instead of the entire `/youtube` directory[^1].
`USE_YTDLP` | -Y | `True` | Allows the user to disable calls to YouTube via `yt-dlp`. This will not allow any calls to YouTube and will instead only search ElasticSearch. 
`YTDLP_SLEEP` | -s | `3` | Number of seconds to wait between each call to YouTube when using `yt-dlp`. Value will not be used if `USE_YTDLP` is set to `False`.
`PERFORM_MIGRATION` | -M | `False` | If set to `False`, this will perform a review of what files need to be migrated and why. If set to `True`, this will attempt to migrate all files[^2]. 
`DEBUG` | -B | `False` | If set to `True`, this will show debugging outputs.
`DRY_RUN` | -r | `False` | If set to `True` and `PERFORM_MIGRATION` is `True`, then it will only show what it expects to change. All details are preceeded with a `DRY_RUN` statement.
`GUESS_TYPES` | -g | `False` | If set to True, will attempt to guess the type of the files by looking at the file itself. Decreases chances of false positives based on file extension, but does access the file and can slow down the analysis.

[^1]: This could cause issues with the migration portion, as it will be relative to the `SOURCE_DIR`.
[^2]: This is a destructive process and could cause issues with files.
  There is a ten second barrier after analysis to allow cancellation of the script before starting the migration process - once it is started, it **should not be interrupted**.


## Running Script
This is expected to run from within the TubeArchivist container, at the `/app` directory. This allows it to see the TubeArchivist helper functions.

You can run this script with the optional flags. For example:
```
python ta_migration_helper.py -d /path/to/custom/directory -Y -s 5 -M
```

This would set the source directory to `/path/to/custom/directory`, disable YouTube calls via `yt-dlp`, set the sleep time to 5 seconds between YouTube calls, and enable migration.
