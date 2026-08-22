# Architecture notes

Production behavior is defined by the current code and tests:

- `internal/session` owns lifecycle decisions, stale and duplicate behavior, intervals, and tag invariants.
- `internal/service` owns raw-to-trusted event creation, login ID generation, event orchestration, and query orchestration.
- `internal/eventstream` owns stable full-key partition routing, bounded FIFO queues, workers, retries, and draining shutdown.
- `internal/repository/sqlite` owns snapshot loading, schema initialization, atomic SQL persistence, persistence validation, SQL predicates, and page loading.
- `internal/query` owns query value normalization, fingerprints, evaluated-at snapshots, and keyset cursors.
- `internal/httpapi` owns strict transport decoding, body limits, HTTP error mapping, and structured dead-letter logging.
- `cmd/server` directly composes the standalone runtime and manages readiness and shutdown.

All production writes follow `HTTP -> trusted event -> Dispatcher.Submit -> owning worker -> repository.LoadCurrent -> domain decision -> repository.ApplyMutation`. There is no alternate stdin, broker, map, or PostgreSQL mutation path.

Partition ownership supplies same-key serialization. The service and repository do not perform application-level locking. SQLite transactions are used only to guarantee atomic persistence and rollback.

This is correct only while every event write enters through `Dispatcher.Submit`, routing uses the canonical `SessionKey`, one goroutine exclusively owns each fixed partition, retries finish before that worker accepts its next event, and one application process writes to the in-memory database. A future multi-process writer requires database optimistic concurrency or exclusive external partition ownership, not an in-process striped mutex.
