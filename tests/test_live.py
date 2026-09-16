"""The live summariser, against the event shapes each harness actually emits."""

import json

from de_bench.live import summarize


def test_claude_code_tool_call_shows_what_it_ran():
    event = {"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "tool_use", "name": "Bash", "input": {"command": "find /work -type f"}}]}}
    assert summarize(json.dumps(event)) == ["tool  Bash: find /work -type f"]


def test_pi_camelcase_tool_call_is_understood():
    """pi spells it toolCall with `arguments`, not tool_use with `input`."""
    event = {"type": "message_end", "message": {"role": "assistant", "content": [
        {"type": "toolCall", "name": "bash", "arguments": {"command": "ls /work"}}]}}
    assert summarize(json.dumps(event)) == ["tool  bash: ls /work"]


def test_pi_tool_call_without_settled_arguments_still_names_the_tool():
    event = {"type": "message_end", "message": {"role": "assistant", "content": [
        {"type": "toolCall", "name": "bash", "arguments": {}}]}}
    assert summarize(json.dumps(event)) == ["tool  bash"]


def test_tool_output_is_not_labelled_as_the_agent_speaking():
    """pi delivers tool output as non-assistant text; calling that "say" would
    make a file listing read as the agent talking."""
    said = summarize(json.dumps({"type": "message_end", "message": {
        "role": "assistant", "content": [{"type": "text", "text": "I will add the DAG"}]}}))
    got = summarize(json.dumps({"type": "message_end", "message": {
        "role": "toolResult", "content": [{"type": "text", "text": "/work /work/dags"}]}}))
    assert said[0].startswith("say")
    assert got[0].startswith("got")


def test_codex_items_are_understood():
    assert summarize(json.dumps({"type": "item.completed", "item": {
        "type": "command_execution", "command": "ls -la"}})) == ["tool  bash: ls -la"]
    assert summarize(json.dumps({"type": "item.completed", "item": {
        "type": "agent_message", "text": "Plan: add the DAG"}})) == ["say  Plan: add the DAG"]


def test_noisy_and_empty_events_produce_nothing():
    for raw in ('{"type": "message_update", "delta": "a"}', '{"type": "session"}',
                "not json at all", "", "   ", "[1,2,3]"):
        assert summarize(raw) == []


def test_empty_thinking_block_is_dropped():
    """Signature-only thinking blocks carry no readable content."""
    event = {"type": "message_end", "message": {"role": "assistant", "content": [
        {"type": "thinking", "thinking": "", "thinkingSignature": "abc"}]}}
    assert summarize(json.dumps(event)) == []


def test_long_text_is_clipped_to_one_line():
    event = {"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "text", "text": "x " * 500}]}}
    out = summarize(json.dumps(event))
    assert len(out) == 1 and len(out[0]) < 200 and "\n" not in out[0]


def test_an_unknown_shape_costs_a_line_not_an_exception():
    assert summarize(json.dumps({"type": "something_new", "payload": {"a": 1}})) == []
