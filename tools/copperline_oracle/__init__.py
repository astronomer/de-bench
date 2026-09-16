"""An independent revenue-recognition model for the copperline world.

The package reads `docs/finance-policy.md` and the raw tables and nothing
else. It does not import, read or mirror `gen_copperline`, the dbt project
or any task. See `recognition.py` for the rules and `AMBIGUITIES.md` for the
places the policy does not decide the answer.
"""
