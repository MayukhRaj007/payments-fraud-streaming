# Screenshots

Captured from a live stack (`docker compose up -d`, producer running ~20
minutes) at a 1680px viewport.

| File | Source |
|---|---|
| `dashboard.png` | <http://localhost:3000> — Grafana, kiosk mode, last 30 minutes |
| `flink-job.png` | <http://localhost:8081> — the running job's overview |
| `kafka-ui.png` | <http://localhost:8082> — the `transactions` topic, Messages tab |
| `evaluate.png` | The output of `make evaluate` |

## A note on `evaluate.png`

This one is **not** a raw terminal capture — it is the text of
[`../evaluation-run.txt`](../evaluation-run.txt) rendered in a terminal-styled
HTML page and screenshotted, because a real terminal capture at this size was
unreadable.

The text is reproduced verbatim; no numbers were edited. If you would rather
read the source, `docs/evaluation-run.txt` is the file `make evaluate` wrote.

## Retaking them

Start the stack, let the producer run for a few minutes so the panels have data,
then capture at a wide viewport (1680px works well). For Grafana, append
`&kiosk` to the dashboard URL to drop the chrome.
