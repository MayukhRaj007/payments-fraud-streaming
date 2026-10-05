-- Schema for the fraud alert sink.
-- Runs automatically on first start via the postgres image's
-- /docker-entrypoint-initdb.d hook. It only executes when the data directory is
-- empty, so edits here need `docker compose down -v` to take effect.

-- ---------------------------------------------------------------- alerts ----
CREATE TABLE IF NOT EXISTS fraud_alerts (
    -- Deterministic uuid5(txn_id, rule) produced by flink_jobs/rules.py. Because
    -- it is derived from the data rather than random, a replayed transaction
    -- generates the identical id. That is what makes the pipeline's
    -- at-least-once delivery safe: the ON CONFLICT upsert in the Flink sink
    -- collapses a duplicate instead of inserting a second row.
    alert_id     UUID        PRIMARY KEY,
    txn_id       UUID        NOT NULL,
    card_id      TEXT        NOT NULL,
    rule         TEXT        NOT NULL,
    severity     TEXT        NOT NULL,
    -- Rule-specific evidence (counts, countries, gaps). JSONB keeps the schema
    -- stable as rules gain fields, and stays queryable unlike a text blob.
    details      JSONB       NOT NULL DEFAULT '{}'::JSONB,
    -- Event time of the transaction that triggered the alert, NOT wall clock.
    -- Everything downstream is measured in event time so a replay reproduces
    -- the same timeline.
    detected_at  TIMESTAMPTZ NOT NULL,
    -- When the row reached Postgres. detected_at - ingested_at is end-to-end
    -- pipeline latency.
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fraud_alerts_rule_check
        CHECK (rule IN ('VELOCITY', 'AMOUNT', 'IMPOSSIBLE_TRAVEL')),
    CONSTRAINT fraud_alerts_severity_check
        CHECK (severity IN ('MEDIUM', 'HIGH')),
    -- Redundant given the deterministic primary key, but it states the rule
    -- explicitly: one transaction raises at most one alert per rule.
    CONSTRAINT fraud_alerts_txn_rule_unique UNIQUE (txn_id, rule)
);

-- Time-ordered reads dominate (dashboards, live feed), so index that first.
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_detected_at
    ON fraud_alerts (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_card_id
    ON fraud_alerts (card_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_rule
    ON fraud_alerts (rule);

COMMENT ON TABLE fraud_alerts IS
    'Alerts emitted by the PyFlink fraud detector. Idempotent on (txn_id, rule).';

-- ----------------------------------------------------------------- views ----

-- Alerts per minute, split by rule. Grafana's time-series panel reads this.
CREATE OR REPLACE VIEW v_alerts_per_minute AS
SELECT
    DATE_TRUNC('minute', detected_at) AS minute,
    rule,
    COUNT(*)                         AS alerts
FROM fraud_alerts
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

COMMENT ON VIEW v_alerts_per_minute IS
    'Alert rate per minute per rule, bucketed on event time.';

-- Which cards are most implicated. `rules` shows whether a card tripped one
-- rule repeatedly or several different ones, which is the more serious signal.
CREATE OR REPLACE VIEW v_top_cards_by_alerts AS
SELECT
    card_id,
    COUNT(*)                                   AS alerts,
    COUNT(DISTINCT rule)                       AS distinct_rules,
    ARRAY_AGG(DISTINCT rule ORDER BY rule)     AS rules,
    SUM((severity = 'HIGH')::INT)              AS high_severity,
    MIN(detected_at)                           AS first_seen,
    MAX(detected_at)                           AS last_seen
FROM fraud_alerts
GROUP BY card_id
ORDER BY alerts DESC, last_seen DESC;

COMMENT ON VIEW v_top_cards_by_alerts IS
    'Cards ranked by alert count; distinct_rules highlights multi-rule cards.';

-- Totals per rule, with severity breakdown and distinct cards affected.
CREATE OR REPLACE VIEW v_alerts_by_rule AS
SELECT
    rule,
    COUNT(*)                          AS alerts,
    COUNT(DISTINCT card_id)           AS cards_affected,
    SUM((severity = 'HIGH')::INT)     AS high_severity,
    SUM((severity = 'MEDIUM')::INT)   AS medium_severity,
    MAX(detected_at)                  AS last_seen
FROM fraud_alerts
GROUP BY rule
ORDER BY alerts DESC;

COMMENT ON VIEW v_alerts_by_rule IS
    'Alert totals per rule with severity split.';

-- Live feed for the dashboard table. Pulls the human-readable bits out of the
-- details JSON so the panel needs no per-rule logic of its own.
CREATE OR REPLACE VIEW v_recent_alerts AS
SELECT
    detected_at,
    rule,
    severity,
    card_id,
    txn_id,
    COALESCE(
        details->>'amount',
        details->>'txn_count',
        details->>'gap_sec'
    )                                       AS metric,
    CASE rule
        WHEN 'AMOUNT'            THEN 'CAD ' || (details->>'amount')
        WHEN 'VELOCITY'          THEN (details->>'txn_count') || ' txns in '
                                      || (details->>'window_sec') || 's'
        WHEN 'IMPOSSIBLE_TRAVEL' THEN (details->>'from_country') || ' -> '
                                      || (details->>'to_country') || ' in '
                                      || (details->>'gap_sec') || 's'
    END                                     AS summary,
    details
FROM fraud_alerts
ORDER BY detected_at DESC;

COMMENT ON VIEW v_recent_alerts IS
    'Most recent alerts with a pre-rendered human-readable summary per rule.';
