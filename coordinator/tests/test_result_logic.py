from internal.queue.result_consumer import _map_status, apply_result


def test_map_status_keeps_known_outcomes() -> None:
    assert _map_status("completed") == "completed"
    assert _map_status("failed") == "failed"
    assert _map_status("timeout") == "failed"
    assert _map_status("cancelled") == "failed"
    assert _map_status("anything-else") == "scheduled"


def test_result_without_lease_is_rejected_before_touching_the_database() -> None:
    result = apply_result(
        None,  # type: ignore[arg-type]
        {
            "taskId": "00000000-0000-0000-0000-000000000001",
            "executionId": "00000000-0000-0000-0000-000000000002",
            "status": "completed",
        },
    )

    assert result.applied is False
    assert result.reason == "missing_lease"
