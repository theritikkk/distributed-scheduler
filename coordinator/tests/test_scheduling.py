from datetime import datetime, timedelta, timezone

from internal.scheduling import (
    compute_dispatch_at,
    next_cron,
    next_future_cron_slot,
    next_run_for_task,
    stable_offset,
)

TASK_ID = "00000000-0000-4000-8000-000000000001"


def test_stable_offset_matches_typescript_values() -> None:
    assert stable_offset(TASK_ID, 0) == 0
    assert stable_offset(TASK_ID, 10) == 7
    assert stable_offset(TASK_ID, 30) == 19
    assert stable_offset(TASK_ID, 59) == 8


def test_compute_dispatch_at_caps_jitter_to_the_period() -> None:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert compute_dispatch_at(base, TASK_ID, 30, None) == base + timedelta(seconds=19)
    assert compute_dispatch_at(base, TASK_ID, 300, 60) == base + timedelta(seconds=8)
    assert compute_dispatch_at(base, TASK_ID, 0, None) == base
    assert (
        compute_dispatch_at(base, TASK_ID, 30, None, enabled=False) == base
    )


def test_missed_slots_are_skipped() -> None:
    scheduled_for = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    now = datetime(2026, 1, 1, 12, 5, 30, tzinfo=timezone.utc)

    next_slot = next_future_cron_slot("* * * * *", scheduled_for, now)

    assert next_slot == datetime(2026, 1, 1, 12, 6, tzinfo=timezone.utc)


def test_recurring_next_run_uses_base_slot_and_stable_jitter() -> None:
    scheduled_for = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    now = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)

    base, dispatch = next_run_for_task(
        TASK_ID,
        "* * * * *",
        scheduled_for,
        now,
        30,
        "stable",
        jitter_enabled=True,
    )

    assert base == datetime(2026, 1, 1, 12, 1, tzinfo=timezone.utc)
    assert dispatch == base + timedelta(seconds=19)


def test_jitter_none_policy_forces_exact_dispatch() -> None:
    scheduled_for = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    now = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)

    base, dispatch = next_run_for_task(
        TASK_ID,
        "* * * * *",
        scheduled_for,
        now,
        30,
        "none",
        jitter_enabled=True,
    )

    assert base == datetime(2026, 1, 1, 12, 1, tzinfo=timezone.utc)
    assert dispatch == base


def test_next_slot_preserves_cadence_when_current_run_is_on_time() -> None:
    scheduled_for = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    now = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)

    next_slot = next_future_cron_slot("* * * * *", scheduled_for, now)

    # The result consumer computes from the base slot, not from wall-clock now.
    assert next_slot == scheduled_for + timedelta(minutes=1)


def test_10k_same_slot_tasks_spread_across_the_jitter_window() -> None:
    offsets = [stable_offset(f"same-slot-task-{i}", 30) for i in range(10_000)]

    assert min(offsets) == 0
    assert max(offsets) == 30
    assert set(offsets) == set(range(31))


def test_invalid_cron_falls_back_to_one_minute() -> None:
    now = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)

    assert next_cron(now, "not a cron expression") == now + timedelta(minutes=1)
