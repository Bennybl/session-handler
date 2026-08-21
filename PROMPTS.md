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
