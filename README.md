# ta_migration_helper
Migration helper for TubeArchivist from the legacy filenaming convention (v0.3.6 and earlier) to the current naming convention (v0.4.0 and later).

This is expected to run from within the TubeArchivist container.

Current functionality:
1. Detects videos in filesystem that are not in ElasticSearch.
2. Detects videos that are in ElasticSearch and not in the filesystem.
3. Provides and output of what those videos are.

Expected next steps:
1. Allow automatic migration for those files that are detected.
2. Allow automatic updates to Elasticsearch.
3. Allow migration of subtitles/other files found.
4. Determine if there are other functions that need to be performed.
