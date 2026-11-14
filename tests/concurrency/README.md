# Concurrency and load harnesses

Two complementary checks:

1. `tests/concurrency/task_submission.py` — N parallel authenticated task-creation
   requests. It verifies every response is `201` and every returned task ID is unique.
2. `coordinator/tests/test_dispatch_and_result.py::test_concurrent_coordinators_create_one_execution`
   — two PostgreSQL connections race to dispatch the same due task. Exactly one creates
   an execution; the other observes `running` and skips.

## API submission harness

```bash
BASE_URL=http://localhost:3000 \
python3 tests/concurrency/task_submission.py --requests 200 --concurrency 40
```

The script registers its own user; no token is required.

## Concurrent coordinator test

```bash
TEST_DATABASE_URL=postgresql:///scheduler_phase0_test \
python3 -m pytest coordinator/tests/test_dispatch_and_result.py \
  -k concurrent_coordinators -q
```

The test creates the schema from `database/init.sql` if the database is empty and skips
when `TEST_DATABASE_URL` is not set.
