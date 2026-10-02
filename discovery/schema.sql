-- One show = one row, however many URLs and catalogue ids it has had.
CREATE SCHEMA IF NOT EXISTS disc;

CREATE TABLE IF NOT EXISTS disc.run (
  run_id      serial PRIMARY KEY,
  started_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  note        text
);

CREATE TABLE IF NOT EXISTS disc.show (
  show_id         serial PRIMARY KEY,
  guid            text UNIQUE,                 -- <podcast:guid> or Podcast Index podcastGuid
  itunes_id       bigint UNIQUE,
  pi_id           bigint UNIQUE,
  title           text,
  author          text,
  feed_url        text NOT NULL,               -- where we fetch it now
  etag            text,
  last_modified   text,
  last_episode_at timestamptz,
  episodes        int,
  active          boolean,                     -- last episode within ACTIVE_DAYS of the run
  first_run       int NOT NULL REFERENCES disc.run,
  last_fetch_run  int REFERENCES disc.run,
  last_fetch_status int
);

-- Every URL a show has been known by. A lookup here is how a moved feed stays the same show.
CREATE TABLE IF NOT EXISTS disc.feed_alias (
  url_key   text PRIMARY KEY,
  show_id   int NOT NULL REFERENCES disc.show,
  url       text NOT NULL,
  via       text NOT NULL,   -- seed | catalogue | 301 | new-feed-url | itunes
  run_id    int NOT NULL REFERENCES disc.run
);

-- What a person should look at: new shows, changes, and matches we would not make alone.
CREATE TABLE IF NOT EXISTS disc.review (
  review_id serial PRIMARY KEY,
  run_id    int NOT NULL REFERENCES disc.run,
  kind      text NOT NULL,   -- new_show | renamed | moved | went_inactive | ambiguous
  show_id   int REFERENCES disc.show,
  detail    jsonb NOT NULL,
  resolved  boolean NOT NULL DEFAULT false
);

-- Per run, per source: how many calls, how they went, how long they took.
CREATE TABLE IF NOT EXISTS disc.source_log (
  run_id       int NOT NULL REFERENCES disc.run,
  source       text NOT NULL,      -- podcastindex | itunes | rss
  requests     int NOT NULL,
  ok           int NOT NULL,
  not_modified int NOT NULL,
  redirected   int NOT NULL,
  errors       int NOT NULL,
  ms_total     bigint NOT NULL,
  PRIMARY KEY (run_id, source)
);
