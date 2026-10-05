"""PyFlink fraud detection job.

Reads `transactions` from Kafka, applies the three rules in event time, and
writes alerts to both the `fraud-alerts` topic and the Postgres `fraud_alerts`
table.

Why DataStream rather than Table API / SQL
------------------------------------------
Rule 3 (impossible travel) needs to remember each card's last card-present
country -- that is keyed state, which is DataStream's native idiom. Expressing it
in SQL means either an awkward self-join or a UDF that smuggles state in anyway.
More importantly, keeping the decision logic in plain Python lets
tests/test_rules.py exercise the *real* rules without a cluster. Rules written as
SQL strings can only be tested by standing Flink up, so in practice they get
tested against a reimplementation -- which is worse than no test, because it can
pass while the job is broken.

This file is therefore deliberately thin: it owns I/O, watermarks and state
*storage*, and delegates every decision to rules.evaluate().
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

from pyflink.common import Duration, Row, SimpleStringSchema, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import KeyedProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

# rules.py sits beside this file in the image and is also shipped to the
# TaskManagers via `flink run -pyfs`.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import rules  # noqa: E402

LOG = logging.getLogger("fraud_detector")

# Alert row layout for the JDBC sink. Order must match INSERT_SQL's parameters.
# Every field travels as a STRING. Keeping the Table layer string-only avoids
# TIMESTAMP_LTZ, which PyFlink cannot carry across the DataStream/Table boundary
# that attach_as_datastream() creates ("Unsupported type:TIMESTAMP_LTZ(3)").
# PostgreSQL does the real typing: with stringtype=unspecified it coerces these
# into UUID, JSONB and TIMESTAMPTZ columns itself.
ALERT_ROW_TYPE = Types.ROW_NAMED(
    ["alert_id", "txn_id", "card_id", "rule", "severity", "details", "detected_at"],
    [Types.STRING()] * 7,
)

# Postgres sink, declared as a Table API JDBC table.
#
# Why the Table API connector and not DataStream's JdbcSink: PyFlink 1.20.5's
# JdbcSink reflects on
#     org.apache.flink.connector.jdbc.internal.JdbcOutputFormat
#         .createRowJdbcStatementBuilder(int[])
# but in every published flink-connector-jdbc for Flink 1.18-1.20 (3.2.0 through
# 3.4.0) that static method lives on RowJdbcOutputFormat instead, so the call
# fails with NoSuchMethodException. Only 3.1.x (built for Flink 1.17) still has
# it where PyFlink looks, and running a 1.17 connector against a 1.20 runtime is
# a binary-compatibility gamble. The Table API connector is version-matched to
# 1.20 and fully supported, so the Postgres write goes through it.
#
# PRIMARY KEY ... NOT ENFORCED makes the connector generate a Postgres upsert
# (INSERT ... ON CONFLICT (alert_id) DO UPDATE). Combined with the deterministic
# uuid5(txn_id, rule) alert_id, duplicate delivery is a no-op -- which is what
# makes at-least-once safe here.
#
# stringtype=unspecified is required: the connector binds STRING columns with
# setString, and PostgreSQL refuses to coerce varchar into uuid or jsonb. With
# it, the parameters are sent untyped and PostgreSQL casts them to the column
# types, so alert_id/txn_id land in UUID columns and details in JSONB.
SINK_DDL = """
CREATE TABLE pg_fraud_alerts (
    alert_id    STRING,
    txn_id      STRING,
    card_id     STRING,
    `rule`      STRING,
    severity    STRING,
    details     STRING,
    detected_at STRING,
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector'                      = 'jdbc',
    'url'                            = '{url}?stringtype=unspecified',
    'table-name'                     = 'fraud_alerts',
    'driver'                         = 'org.postgresql.Driver',
    'username'                       = '{user}',
    'password'                       = '{password}',
    'sink.buffer-flush.max-rows'     = '50',
    'sink.buffer-flush.interval'     = '1s',
    'sink.max-retries'               = '3'
)
"""

# A straight pass-through: all typing happens in PostgreSQL. ingested_at is
# deliberately absent so Postgres fills it with NOW(), making
# ingested_at - detected_at the end-to-end pipeline latency.
INSERT_INTO_PG = """
INSERT INTO pg_fraud_alerts
SELECT alert_id, txn_id, card_id, `rule`, severity, details, detected_at
FROM alerts
"""


def load_config() -> dict:
    """Job configuration from the environment; see .env.example."""
    env = os.environ.get
    return {
        "bootstrap": env("KAFKA_BOOTSTRAP", "kafka:9092"),
        "topic_in": env("TOPIC_TRANSACTIONS", "transactions"),
        "topic_out": env("TOPIC_ALERTS", "fraud-alerts"),
        "group_id": env("CONSUMER_GROUP", "fraud-detector"),
        "parallelism": int(env("FLINK_PARALLELISM", "2")),
        "watermark_lateness_sec": int(env("WATERMARK_LATENESS_SEC", "5")),
        "checkpoint_interval_ms": int(env("CHECKPOINT_INTERVAL_MS", "10000")),
        # Rule thresholds, converted to the millisecond units rules.py expects.
        "rules": {
            "velocity_window_ms": int(env("VELOCITY_WINDOW_SEC", "60")) * 1000,
            "velocity_max_txns": int(env("VELOCITY_MAX_TXNS", "5")),
            "amount_threshold_cad": float(env("AMOUNT_THRESHOLD_CAD", "5000")),
            "travel_window_ms": int(env("TRAVEL_WINDOW_SEC", "600")) * 1000,
        },
        "pg_url": "jdbc:postgresql://{}:{}/{}".format(
            env("POSTGRES_HOST", "postgres"),
            env("POSTGRES_PORT", "5432"),
            env("POSTGRES_DB", "fraud"),
        ),
        "pg_user": env("POSTGRES_USER", "fraud"),
        "pg_password": env("POSTGRES_PASSWORD", "fraud"),
    }


class TxnTimestampAssigner(TimestampAssigner):
    """Pulls event time out of each transaction's JSON payload.

    Event time, not ingestion time: a replay of the same Kafka data must produce
    an identical alert timeline, and the velocity/travel windows have to measure
    when transactions *happened*, not when Flink happened to read them.
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return rules.parse_event_time(json.loads(value)["event_time"])
        except (ValueError, KeyError, TypeError):
            # A malformed record must not stall the watermark or kill the job.
            # Falling back to the Kafka record timestamp keeps time moving; the
            # record is then dropped in process_element.
            return record_timestamp


class FraudDetector(KeyedProcessFunction):
    """Holds per-card state and delegates all decisions to rules.evaluate().

    Keyed by card_id, so each card's transactions arrive at one instance in
    order, which is exactly what the velocity and travel rules need.
    """

    def __init__(self, rule_cfg: dict):
        # Pickled with the function and shipped to the TaskManagers.
        self.rule_cfg = rule_cfg
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        # CardState round-trips as a plain dict, so the default pickle
        # serialiser is enough and no custom TypeSerializer is needed.
        self.state = runtime_context.get_state(
            ValueStateDescriptor("card_state", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        try:
            txn = json.loads(value)
            state = rules.CardState.from_dict(self.state.value())
            alerts, state = rules.evaluate(txn, state, self.rule_cfg)
            self.state.update(state.to_dict())
        except Exception as exc:  # noqa: BLE001 - one bad record must not kill the job
            LOG.warning("skipping unparseable record: %s", exc)
            return
        for alert in alerts:
            yield json.dumps(alert)


def alert_to_row(alert_json: str) -> Row:
    """Alert JSON -> Row matching ALERT_ROW_TYPE.

    detected_at_ms becomes an ISO-8601 UTC string rather than a Flink timestamp:
    PostgreSQL parses it into TIMESTAMPTZ on insert, which keeps TIMESTAMP_LTZ
    out of the Python type system entirely.
    """
    a = json.loads(alert_json)
    detected_at = datetime.fromtimestamp(int(a["detected_at_ms"]) / 1000, timezone.utc)
    return Row(
        alert_id=a["alert_id"],
        txn_id=a["txn_id"],
        card_id=a["card_id"],
        rule=a["rule"],
        severity=a["severity"],
        details=json.dumps(a["details"]),
        detected_at=detected_at.isoformat(),
    )


def build_source(cfg: dict) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(cfg["bootstrap"])
        .set_topics(cfg["topic_in"])
        .set_group_id(cfg["group_id"])
        # Start from the beginning of the topic. Reprocessing is harmless here
        # because alert_ids are deterministic and the sink upserts, and it means
        # a job started after the producer still sees every transaction.
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_alert_kafka_sink(cfg: dict) -> KafkaSink:
    """At-least-once rather than exactly-once, deliberately.

    Exactly-once would need Kafka transactions, which hold alerts invisible
    until a checkpoint completes -- adding up to a full checkpoint interval of
    latency to a fraud alert. Because alert_id is deterministic, a duplicate is
    idempotent at every consumer that keys on it, so the cost of a duplicate is
    near zero while the cost of latency is real. See the README.
    """
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(cfg["bootstrap"])
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(cfg["topic_out"])
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def attach_postgres_sink(env, alert_rows, cfg: dict) -> None:
    """Wire the alert stream into Postgres via the Table API JDBC connector.

    `attach_as_datastream()` adds the INSERT to *this* job graph rather than
    launching a second job, so one env.execute() runs both sinks.
    """
    t_env = StreamTableEnvironment.create(env)
    t_env.create_temporary_view("alerts", t_env.from_data_stream(alert_rows))
    t_env.execute_sql(
        SINK_DDL.format(
            url=cfg["pg_url"], user=cfg["pg_user"], password=cfg["pg_password"]
        )
    )
    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(INSERT_INTO_PG)
    statement_set.attach_as_datastream()


def main() -> None:
    cfg = load_config()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(cfg["parallelism"])
    # Checkpointing gives the keyed state (and Kafka offsets) recovery
    # semantics; without it a TaskManager restart loses every card's history.
    env.enable_checkpointing(cfg["checkpoint_interval_ms"])

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        # 5s of tolerated out-of-orderness. The producer schedules anomalies
        # forward in time precisely so they stay inside this bound.
        Duration.of_millis(cfg["watermark_lateness_sec"] * 1000)
    ).with_timestamp_assigner(TxnTimestampAssigner())

    transactions = env.from_source(
        build_source(cfg), watermark_strategy, "kafka: transactions"
    )

    alerts = (
        transactions
        # Keying by card_id is what makes per-card state correct: every
        # transaction for a card reaches the same operator instance, in order.
        .key_by(lambda record: json.loads(record)["card_id"], key_type=Types.STRING())
        .process(FraudDetector(cfg["rules"]), output_type=Types.STRING())
        .name("fraud rules")
    )

    # Fan out to both sinks from the same stream.
    alerts.sink_to(build_alert_kafka_sink(cfg)).name("kafka: fraud-alerts")
    attach_postgres_sink(
        env, alerts.map(alert_to_row, output_type=ALERT_ROW_TYPE), cfg
    )

    env.execute("payments-fraud-detector")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    main()
