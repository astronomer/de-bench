"""Run-validity rules: what counts as the infrastructure failing, not the agent.

Both directions cost real money when they are wrong. A false positive files a
finished trial as broken and invites a re-run; a false negative lets a harness
that refused to work be scored as a harness that tried and failed.
"""

from de_bench.validity import classify_outcome

PATCH = 1200  # a trial that left real work behind


def outcome(transcript="", stderr="", patch=PATCH, status="completed"):
    return classify_outcome(status, transcript, stderr, patch)


def test_a_ticket_number_is_not_an_auth_failure():
    """`Ticket UP-401` in the prompt echo filed 13 finished trials as infra."""
    prompt = "Ticket UP-401: this project builds its DAGs from YAML configs"
    assert outcome(transcript=prompt) == ("scored", [])


def test_agent_source_mentioning_401_is_not_an_auth_failure():
    assert outcome(stderr="if exc.code != 401:\n    raise") == ("scored", [])


def test_a_real_401_still_counts():
    assert outcome(stderr="HTTP 401 Unauthorized")[0] == "infra_failed"
    assert outcome(stderr="Unauthorized (401)")[0] == "infra_failed"
    assert outcome(stderr="403 forbidden")[0] == "infra_failed"


def test_a_missing_harness_binary_counts_but_a_missing_tool_does_not():
    assert outcome(stderr="bash: codex: command not found")[0] == "infra_failed"
    # Agents probe for things nobody promised them, and echo their own probes.
    assert outcome(stderr="bash: jq: command not found") == ("scored", [])
    assert outcome(stderr="else echo 'airflow command not found'; fi") == ("scored", [])


def test_a_permission_refusal_that_left_nothing_is_infra():
    """opencode auto-rejected every path outside /work and died on the spot.

    The transcript is long — the harness streams a full event log on its way to
    doing nothing, which is exactly why this hid for two runs.
    """
    stderr = "! permission requested: external_directory (/root/.af/*); auto-rejecting"
    verdict, reasons = outcome(transcript="{}" * 200, stderr=stderr, patch=0)
    assert verdict == "infra_failed"
    assert "blocked_on_permission" in reasons


def test_a_permission_refusal_is_read_from_the_transcript_too():
    """pi and claude-code write no stderr at all, so stderr-only saw nothing."""
    assert outcome(transcript="The user rejected permission to use this specific tool call.",
                   patch=0)[0] == "infra_failed"


def test_a_refusal_the_agent_worked_around_is_not_infra():
    """otto refuses commands it cannot statically analyze, then rewrites them."""
    assert outcome(stderr="Approval needed: /tmp sits outside the project working directory",
                   patch=PATCH) == ("scored", [])


def test_spending_the_budget_is_capability_evidence():
    assert outcome(transcript="", patch=0, status="wall_cap") == ("scored", [])
