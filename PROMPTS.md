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
