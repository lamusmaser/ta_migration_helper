# ta_migration_helper
Migration helper for TubeArchivist from the legacy filenaming convention (v0.3.6 and earlier) to the current naming convention (v0.4.0 and later).

This is expected to run from within the TubeArchivist container, at the `/app` directory. This allows it to see the TubeArchivist helper functions.

Current functionality:
1. Detects videos in filesystem that are not in ElasticSearch.
2. Detects videos that are in ElasticSearch and not in the filesystem.
3. Provides and output of what those videos are.

Expected next steps:
1. Allow automatic migration for those files that are detected.
2. Allow automatic updates to Elasticsearch.
3. Allow migration of subtitles/other files found.
4. Determine if there are other functions that need to be performed.

## Environment Variables
Variable | Default | Purpose
:--- | :---: | :---
`SOURCE_DIR` | `/youtube` | The source directory that will be searched for videos that need to be migrated. This can be used to specify an individual folder instead of the entire `/youtube` directory. 
`USE_YTDLP` | `True` | Allows the user to disable calls to YouTube via `yt-dlp`. This will not allow any calls to YouTube and will instead only search ElasticSearch. 
`YTDLP_SLEEP` | `3` | Number of seconds to wait between each call to YouTube when using `yt-dlp`. Value will not be used if `USE_YTDLP` is set to `False`.
`PERFORM_MIGRATION` | `False` | If set to `False`, this will perform a review of what files need to be migrated and why. If set to `True`, this will attempt to migrate all files. 

`SOURCE_DIR` Note: This could cause issues with the migration portion, as it will be relative to the `SOURCE_DIR`.
Migration Note: This is a destructive process and could cause issues with files. There is a ten second barrier after analysis to allow cancellation of the script before starting the migration process - once it is started, it should not be interrupted.