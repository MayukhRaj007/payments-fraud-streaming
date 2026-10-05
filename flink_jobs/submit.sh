#!/usr/bin/env bash
# Submits the PyFlink job once the JobManager is accepting work.
# Runs as a one-shot compose service so `docker compose up` yields a running job
# with no manual step.
set -euo pipefail

JM="${JOBMANAGER_HOST:-flink-jobmanager}:8081"

echo "[submit] waiting for JobManager at ${JM} ..."
for _ in $(seq 1 60); do
  if curl -fsS "http://${JM}/overview" >/dev/null 2>&1; then
    echo "[submit] JobManager is up"
    break
  fi
  sleep 2
done

# Refuse to submit a second copy: compose restarts would otherwise stack
# duplicate jobs that all compete for the same consumer group.
RUNNING=$(curl -fsS "http://${JM}/jobs/overview" 2>/dev/null || echo '{}')
if echo "$RUNNING" | grep -q '"state":"RUNNING"'; then
  echo "[submit] a job is already RUNNING; nothing to do"
  echo "$RUNNING"
  exit 0
fi

echo "[submit] submitting fraud_detector.py ..."
# -pyfs ships rules.py to the TaskManagers and puts it on their PYTHONPATH.
# Without it the job pickles fine on the client and then fails on the workers
# with ModuleNotFoundError: rules.
/opt/flink/bin/flink run \
  --jobmanager "${JM}" \
  --detached \
  --python /opt/pfs/fraud_detector.py \
  --pyFiles /opt/pfs/rules.py

echo "[submit] submitted. Current jobs:"
curl -fsS "http://${JM}/jobs/overview"
