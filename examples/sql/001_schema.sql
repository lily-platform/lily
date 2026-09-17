-- Applied once by the PostgreSQL image on a new examples volume.
CREATE TABLE example_jobs (
    id text PRIMARY KEY,
    text text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 2000),
    result text NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE example_processed_events (
    job_id text PRIMARY KEY REFERENCES example_jobs(id),
    processed_at timestamptz NOT NULL DEFAULT now()
);
