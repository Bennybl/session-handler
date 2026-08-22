# Architecture notes

Production behavior is defined by the current code and tests:

- `internal/session` owns lifecycle decisions, stale and duplicate behavior, intervals, and tag invariants.
- `internal/service` owns raw-to-trusted event creation, login ID generation, event orchestration, and query orchestration.
- `internal/eventstream` owns the broker-neutral source contract, shared envelope decoding, domain-aware retries, and processing policy.
- `internal/eventstream/kafka` owns Kafka consumer-group polling, partition workers, rebalance control, and commit-after-processing offset management.
- `internal/repository/sqlite` owns snapshot loading, schema initialization, atomic SQL persistence, persistence validation, SQL predicates, and page loading.
- `internal/query` owns query value normalization, fingerprints, evaluated-at snapshots, and keyset cursors.
- `internal/httpapi` owns session-query HTTP decoding and response mapping.
- `cmd/server` directly composes the standalone runtime and manages readiness and shutdown.

All production writes follow `Kafka partition worker -> shared strict decoder and retries -> SessionService.ApplyEvent -> repository.LoadCurrent -> domain decision -> repository.ApplyMutation`. Kafka is isolated behind `eventstream.Source`, so another event service can replace it at the composition root without changing the processor, service, domain, or repository. A replacement source must provide the same per-partition sequential-processing and acknowledgment guarantees. HTTP exposes queries and operational probes only; there is no HTTP write path.

Kafka uses manual consumer-group commits and blocks rebalancing while a polled batch is processed. Records from one partition are handled sequentially; records from different partitions may be handled concurrently. The batch is committed only after all handlers succeed. Invalid or permanently rejected messages are dead-letter logged and treated as handled; transient failures leave the batch uncommitted and stop the runtime for safe redelivery. Kafka producers must use a stable key derived from the complete canonical `SessionKey` to preserve same-key ordering.

Partition ownership supplies same-key serialization. The service and repository do not perform application-level locking. SQLite transactions are used only to guarantee atomic persistence and rollback.

This is correct only while every event write enters through `eventstream.Source`, producers route with the complete canonical `SessionKey`, the broker partition count remains fixed, one worker exclusively processes each assigned partition, retries finish before that worker accepts its next event, and one application process writes to the in-memory database. A future multi-process writer requires database optimistic concurrency or exclusive external partition ownership, not an in-process striped mutex.
