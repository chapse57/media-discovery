@echo off
rem Copy to env.cmd (git-ignored), fill in the two keys, then: call env.cmd
rem Keys: https://api.podcastindex.org/signup (free). Never paste them into chat or commit them.
set PODCASTINDEX_KEY=
set PODCASTINDEX_SECRET=
rem Same Postgres container as x-metrics, its own database.
set DISCOVERY_DSN=postgresql://xmetrics:xmetrics@localhost:5432/discovery
