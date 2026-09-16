Inspect this project's pinned dependencies. Identify any
provider combinations that would conflict at install time
before the team plans an upgrade. Write your finding to
`output.json` at the project root with keys `compatible`
(boolean) and `conflicts` (a list of short strings naming
each conflict — empty list when compatible). Do not modify
any existing files.
