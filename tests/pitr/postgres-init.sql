-- Baseline data captured by the base backup. Rows 1-10 must be present after
-- every restore target; later batches are added by run.sh.
CREATE TABLE pitr_events (
    id integer PRIMARY KEY,
    note text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO pitr_events (id, note)
SELECT value, 'baseline'
FROM generate_series(1, 10) AS value;
