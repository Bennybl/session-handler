# Architecture notes

Production behavior is defined by the current code and tests:

- `internal/session` owns lifecycle decisions, stale and duplicate behavior, intervals, and tag invariants.
- `internal/service` owns raw-to-trusted event creation, login ID generation, mutation guarding, and query orchestration.
- `internal/eventstream` owns stable full-key partition routing, bounded FIFO queues, workers, retries, and draining shutdown.
- `internal/repository/sqlite` owns schema initialization, atomic SQL mutations, persistence validation, SQL predicates, and page loading.
- `internal/query` owns query value normalization, fingerprints, evaluated-at snapshots, and keyset cursors.
- `internal/httpapi` owns strict transport decoding, body limits, HTTP error mapping, and structured dead-letter logging.
- `cmd/server` directly composes the standalone runtime and manages readiness and shutdown.

All production writes follow `HTTP -> trusted event -> dispatcher -> worker -> service -> repository transaction`. There is no alternate stdin, broker, map, or PostgreSQL mutation path.
