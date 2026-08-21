# AI Prompt Log

This is a concise running summary of the prompts used while designing and
implementing the assignment.

1. Read `HA.pdf` and plan a simple Go architecture covering the data model,
   components, API entry mode, and concurrency without writing code.
2. Keep in-memory storage behind an abstraction so a database implementation can
   be added later, while avoiding unnecessary complexity.
3. Split implementation into small reviewable steps, each on its own branch with
   one commit and pull request.
4. Save the architecture and implementation steps in separate plan files.
5. Replace raw-event retention with a session-lifecycle model containing
   historical `SessionState` intervals.
6. Support activity-time and login-time filtering, with activity defaulting to
   the current time.
7. Evaluate the best database for temporal session queries and concurrency.
8. Support both a zero-dependency in-memory mode and a persistent PostgreSQL mode
   selected at runtime.
9. Require strict test-first delivery for every code step: write tests, run and
   confirm failure, implement, then rerun and confirm success.
10. Add cursor pagination, clarify tenant-scoped username identity and non-unique
    IP addresses, define atomic `SessionRepository.Mutate`, and use generic
    field/operator/value query specifications backed by adapter registries.
11. Begin implementation one step at a time and stop after each pull request for
    review.
12. After Step 0 was approved and merged, implement only Step 1 using the strict
    test-first cycle: write the session-domain tests, run and confirm the expected
    failure, implement the minimum domain behavior, rerun until green, and open a
    dedicated PR.
13. Replace the generic event `Command` with typed `LoginCommand`,
    `UpdateCommand`, and `LogoutCommand` inputs and separate decision functions;
    make the change locally without pushing it yet.
14. After the typed-command PR was approved and merged, implement only Step 2:
    define the atomic `SessionRepository.Mutate(ctx, key, fn)` boundary and a
    reusable test contract for future memory and PostgreSQL adapters, using the
    required red-then-green workflow.
15. After the repository-contract PR was approved and merged, implement only
    Step 3: create the generic query registry, typed value normalization,
    deterministic query fingerprints, versioned adapter-bound cursors, page-limit
    handling, and stable session sort keys using tests first.
16. Before merging Step 3, make typed `IntervalValue` inputs use the same
    timestamp validation as map inputs: reject zero, equal, and reversed bounds,
    preserve open-ended intervals, and normalize accepted bounds to UTC.
17. After Step 3 was approved and merged, implement only Step 4 with tests first:
    add the in-memory repository using atomic locked mutations, isolated copies,
    declarative generic predicates, same-state temporal/tag matching, stable
    cursor pagination, complete histories, and concurrency coverage.
18. After Step 4 was approved and merged, implement only Step 5 with tests first:
    add a transactional embedded PostgreSQL migration runner, the session-key,
    lifecycle, and state schema with generated ranges and indexes, identity and
    active-lifecycle constraints, and a Compose-managed PostgreSQL service.
19. Revise the PostgreSQL design to remove the `session_keys` domain table. Keep
    only `sessions` and `session_states`, and serialize mutations using a
    transaction-scoped advisory lock derived from `(tenantId, username, ip)`
    before locking and reading matching session rows.
20. After the revised PostgreSQL schema PR was approved and merged, implement
    only Step 6 with tests first: add pooled PostgreSQL repository connections,
    per-session-key `pg_advisory_xact_lock`, matching row locks, isolated snapshot
    loading, transactional typed mutations, rollback, retries, persistence, and
    context cancellation.
21. Refine Step 6 review behavior: prefer `ctx.Err()` when cancellation happens
    during retry handling, explicitly forbid external callback side effects
    because callbacks may be retried, and include state `valid_to` values when
    deriving the snapshot's latest event timestamp.
22. Revise the architecture and implementation steps so events are consumed from
    a stream rather than an HTTP endpoint. Keep HTTP query-only, provide a
    zero-dependency stdin stream and durable NATS JetStream adapter, and account
    for acknowledgement, redelivery, ordering, and atomic event-ID deduplication.
23. After the PostgreSQL mutation PR was approved and merged, implement only
    Step 7 with tests first: add PostgreSQL declarative registry queries,
    same-state predicates, parameter binding, distinct keyset pagination, and
    complete session-history loading, then open its dedicated PR.
24. After the PostgreSQL query PR was approved and merged, implement only Step
    8 with tests first: require UUID event IDs, normalize and propagate them,
    atomically persist the latest ID in both adapters, and make immediate stream
    redelivery a successful duplicate no-op without retaining raw events.
25. Start Step 9 locally without committing or pushing: add the storage-neutral
    application service with test-first event dispatch, normalization, stable
    session UUID generation, duplicate handling, generic query forwarding,
    clock capture, cancellation, and error propagation.
26. Start Step 10 locally without committing or pushing: add the test-first
    transport-neutral consumer, stdin NDJSON source, durable NATS JetStream
    source, explicit post-commit acknowledgements, delayed retry, dead lettering,
    termination, bounded retention, and ordered one-message delivery.
27. Start Step 11 locally without committing or pushing: add the test-first,
    query-only HTTP API with strict generic filter decoding, pagination, complete
    history responses, default-now behavior, and no event-ingestion route.
28. Start Step 12 locally without committing or pushing: add test-first runtime
    configuration and adapter selection, health/readiness, graceful lifecycle
    management, runnable server and migration binaries, and ordered Compose
    startup for PostgreSQL and NATS.
