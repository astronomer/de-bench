"""The prose judge's fact key: complete, well-formed, and attached at load."""

from de_bench.tasks import load_tasks


def _copperline_tasks():
    return [t for t in load_tasks(include_drafts=True) if t.world.name == "copperline"]


def test_every_prose_check_carries_its_fact_key():
    """A RESPONSE.md file_contains check without judge_facts would silently
    fall back to the regex path; loading already refuses partial coverage per
    task, and this holds the corpus-wide expectation."""
    armed = unarmed = 0
    for t in _copperline_tasks():
        for c in t.checks:
            if c.get("kind") == "file_contains" and c.get("path") == "RESPONSE.md":
                if c.get("judge_facts"):
                    armed += 1
                else:
                    unarmed += 1
    assert armed >= 159, f"only {armed} prose checks carry judge_facts"
    assert unarmed == 0, f"{unarmed} prose checks would fall back to regex"


def test_fact_keys_are_well_formed():
    for t in _copperline_tasks():
        for c in t.checks:
            facts = c.get("judge_facts")
            if not facts:
                continue
            assert facts["groups"], f"{t.id}: empty fact groups"
            for g in facts["groups"]:
                alts = g["any_of"] if isinstance(g, dict) else [g]
                assert alts and all(isinstance(a, str) and a.strip() for a in alts), (
                    f"{t.id}: malformed fact group"
                )
