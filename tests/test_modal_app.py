"""The Modal module has to import on its own.

Nothing else in the suite imports it — it pulls in `modal` and defines the images —
so a module-level mistake in it used to surface only when a run started. One did:
a dict of expected harness versions was written above the constants it read, and
`de-bench run` died on `NameError` after the images had already been resolved.
"""

from de_bench.agents import AGENTS


def test_modal_app_imports():
    import de_bench.modal_app  # noqa: F401

#: Harnesses built from a local bundle rather than installed from a pinned release.
LOCALLY_BUILT: set[str] = set()
def test_every_agent_has_a_pinned_version_to_check():
    """A pin in the image is a claim; this is the thing that reads it back."""
    from de_bench.modal_app import EXPECTED_VERSIONS

    assert set(EXPECTED_VERSIONS) == set(AGENTS) - LOCALLY_BUILT, (
        "every installed harness needs an expected version"
    )
    for agent, version in EXPECTED_VERSIONS.items():
        assert version and version[0].isdigit(), f"{agent}: {version!r} is not a version"


def test_locally_built_agents_exist():
    """A name left in LOCALLY_BUILT after its agent is gone would silently exempt
    nothing, and the next agent to take that name would inherit the exemption."""
    assert LOCALLY_BUILT <= set(AGENTS), f"unknown agent(s): {LOCALLY_BUILT - set(AGENTS)}"

def test_the_run_state_the_diff_ignores():
    """Excludes are load-bearing: .astro/ alone was 184KB of Airflow state in one diff.

    Read from the source text, since the list is a local inside the Modal function.
    That makes this a guard against deleting a line, not a test of behaviour — the
    behaviour is only observable in a container.
    """
    from de_bench.tasks import repo_root

    source = (repo_root() / "src" / "de_bench" / "modal_app.py").read_text()
    for pattern in ("*.duckdb", "target/**", "logs/**", "__pycache__/**", ".astro/**",
                    "landing/**", "_notifications/**", "include/data/**"):
        assert f":(exclude,glob)**/{pattern}" in source, f"{pattern} dropped out of the excludes"
