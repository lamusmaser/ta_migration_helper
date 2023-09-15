# ta_migration_helper
Migration helper for TubeArchivist

This is expected to run from within the TubeArchivist container.

Current functionality:
Detects videos in filesystem that are not in ElasticSearch.
Detects videos that are in ElasticSearch and not in the filesystem.
Provides and output of what those videos are.

Expected next steps:
Allow automatic migration for those files that are detected.
Allow automatic updates to Elasticsearch.
Determine if there are other functions that need to be performed.
