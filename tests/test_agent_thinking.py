"""Every agent must pass its thinking level through to the tool it drives.

A config is an (agent, model, thinking) triple. If an agent silently drops the
thinking dimension, the harness still labels the run "high" or "low" everywhere
downstream — results.json, the leaderboard, the artifact — while the tool ran at
its own default. That is invisible in the output and only shows up as a
suspiciously null result.

It has happened once: claude-code never passed `--effort`, so every claude-code
run in the copperline sweep executed at the CLI default (medium) whatever the
config said, and a published "high thinking" config turned out to be a duplicate
of the medium one.
"""

from __future__ import annotations

import pytest

from de_bench.agents import claude_code, codex, opencode, pi

#: Agents whose thinking level must produce a different invocation at every
#: level the sweep uses.
FULL_RANGE = [
    ("claude-code", claude_code),
    ("codex", codex),
    ("pi", pi),
]

LEVELS = ["low", "medium", "high"]


def _argv(mod, thinking: str) -> str:
    return "\n".join(mod.build_argv("PROMPT", "SYSTEM", "MODEL", thinking))


@pytest.mark.parametrize("name,mod", FULL_RANGE)
def test_thinking_reaches_the_argv(name, mod):
    """Each level builds a distinct invocation — no two levels collide."""
    seen = {}
    for level in LEVELS:
        argv = _argv(mod, level)
        clash = seen.get(argv)
        assert clash is None, (
            f"{name}: thinking={level!r} builds the same invocation as "
            f"{clash!r}. The level is being dropped, so runs labelled "
            f"{level!r} would actually execute at the tool's default."
        )
        seen[argv] = level


@pytest.mark.parametrize("name,mod", FULL_RANGE)
def test_thinking_value_appears_verbatim(name, mod):
    """The level itself shows up in the invocation, not just some difference."""
    for level in LEVELS:
        assert level in _argv(mod, level), (
            f"{name}: thinking={level!r} does not appear anywhere in the argv"
        )


def test_opencode_only_has_high_and_max():
    """opencode is the one documented exception.

    Its anthropic models define only "high" and "max" variants; anything else
    runs without extended thinking, so low and medium legitimately collide.
    Pinned here so the exception stays deliberate rather than becoming a
    silently tolerated version of the claude-code bug.
    """
    low = _argv(opencode, "low")
    medium = _argv(opencode, "medium")
    high = _argv(opencode, "high")
    maximum = _argv(opencode, "max")

    assert low == medium, "opencode: low and medium are both no-variant runs"
    assert "--variant high" in high
    assert "--variant max" in maximum
    assert high != maximum


def test_claude_code_passes_effort():
    """Regression test for the specific bug.

    The CLI's knob is `--effort`, taking the same five levels this harness uses.
    An earlier version set MAX_THINKING_TOKENS for xhigh/max only and passed
    nothing at all for low/medium/high.
    """
    for level in ["low", "medium", "high", "xhigh", "max"]:
        argv = claude_code.build_argv("P", "S", "claude-opus-5", level)
        assert "--effort" in argv, f"--effort missing for thinking={level!r}"
        assert argv[argv.index("--effort") + 1] == level

    # No env-var wrapper: --effort supersedes MAX_THINKING_TOKENS, and an env
    # var would take precedence over the flag if both were set.
    argv = claude_code.build_argv("P", "S", "claude-opus-5", "max")
    assert argv[0] == "claude"
    assert not any("MAX_THINKING_TOKENS" in part for part in argv)
