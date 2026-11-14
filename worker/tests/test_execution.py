from main import _max_retries, _truncate_output, execute_task


def test_execute_task_returns_combined_output() -> None:
    output, error = execute_task('{"command": "printf hello"}')

    assert output == "hello"
    assert error is None


def test_execute_task_rejects_missing_command() -> None:
    output, error = execute_task('{"not_command": "true"}')

    assert output == ""
    assert error is None


def test_max_retries_uses_message_then_default() -> None:
    assert _max_retries({"maxRetries": 7}) == 7
    assert _max_retries({}) == 3
    assert _max_retries({"maxRetries": "not-a-number"}) == 3


def test_truncate_output_caps_large_results() -> None:
    output = _truncate_output("x" * 2_000, limit=1_024)

    assert output.startswith("x" * 1_024)
    assert "output truncated" in output
    assert len(output) < 2_000
