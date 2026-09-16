"""Running one step of the legacy estate.

Thirteen of the twenty-two nightly Pentaho jobs still run, and the SSIS
package and the AutoSys calendar with them. None of it was ported. Airflow
drives it from here: one step per task, over SSH, on the old box.

**One step per task.** That is the supply-chain team's rule and this module
exists to make it cheap. A step that fails retries alone, and the graph shows
which step it was. A task that runs a whole job hides seven steps behind one
red square.

**Variables flow forward and this module does not invent them.** A PDI job
carries a variable block from step to step: `set_constants` fixes the staging
schema and the watermark column, `resolve_load_control` reads `ops.load_control`
and sets four variables for the night, and each load step consumes them.
`run_step` returns the block a step produced, and the caller passes it into the
next step. It does not read `ops.load_control` itself, it does not default a
missing variable, and it does not re-resolve one that is already set — because
a step that resolves its own watermark loads a window a sibling step has
already loaded, and nothing about that is visible until the numbers are
double.

**`history_complete` brackets the job, not the step.** It runs once, after the
last load step, and it closes the bracket for the whole job. Running it after
each step marks the history complete while most of it is still missing.

The runner is a stub. It builds the vendor's own command line, runs it over
SSH, and hands back the exit code, the variable block and the log. It does no
retrying, no parsing of vendor XML, and no interpretation of what a step
means. The artifacts under `legacy/` are the description of the work; this is
only how it gets started.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

__all__ = ["LEGACY_HOST", "LEGACY_ROOT", "StepResult", "LegacyStepError", "run_step"]

#: The box the estate still runs on.
LEGACY_HOST = "pdi01.copperline.internal"

#: Where the vendor tools live on that box.
LEGACY_ROOT = "/opt/legacy"

#: A step announces what it set by printing `VAR <name>=<value>`.
_VARIABLE_LINE = re.compile(r"^VAR\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)$")

#: How each kind of artifact is started.
_COMMANDS = {
    ".kjb": "{root}/pdi/kitchen.sh -file={artifact} -level=Basic",
    ".ktr": "{root}/pdi/pan.sh -file={artifact} -level=Basic",
    ".dtsx": "{root}/ssis/dtexec.sh /F {artifact}",
    ".jil": "{root}/autosys/sendevent -E FORCE_STARTJOB -J {step}",
}


class LegacyStepError(RuntimeError):
    """A step exited non-zero. Carries the `StepResult` on `.result`."""

    def __init__(self, result: "StepResult"):
        super().__init__(f"{result.step} exited {result.exit_code}")
        self.result = result


@dataclass(frozen=True)
class StepResult:
    """What one step did.

    `variables` is the block the step printed, and it is what the next step
    needs. An empty block is not an error and not a default: it means the step
    set nothing, which on `set_constants` is the whole of the April control
    break.
    """

    step: str
    artifact: str
    exit_code: int
    variables: dict[str, str] = field(default_factory=dict)
    log: str = ""

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def run_step(
    artifact: str | Path,
    step: str,
    *,
    variables: Mapping[str, str] | None = None,
    host: str = LEGACY_HOST,
    timeout: int = 3600,
    check: bool = True,
) -> StepResult:
    """Run one step of a legacy artifact and return what it did.

    Args:
        artifact: the file under `legacy/`, e.g. `legacy/pdi/trn/load_inventory.ktr`,
            `legacy/ssis/SupplierMaster.dtsx` or `legacy/autosys/copperline.jil`.
            The suffix picks the vendor command.
        step: the step's name — the transform, the executable or the JIL box.
            It is the task id too, so the graph reads like the job.
        variables: the block from the step before, passed in as `-param:`
            arguments. Pass exactly what you were given; adding a variable the
            job did not set makes this run disagree with the nightly one.
        host: the box the estate runs on.
        timeout: seconds before the step is killed.
        check: raise `LegacyStepError` on a non-zero exit. Turn it off only
            where a non-zero exit is a real outcome, such as an AutoSys box
            that reports "nothing due today".

    Returns a `StepResult`. Feeding the next step is `run_step(..., variables=
    result.variables)`.
    """
    artifact = Path(artifact)
    template = _COMMANDS.get(artifact.suffix)
    if template is None:
        raise ValueError(f"no legacy command for {artifact.suffix!r}: {artifact}")

    command = template.format(root=LEGACY_ROOT,
                              artifact=shlex.quote(_remote_path(artifact)),
                              step=shlex.quote(step))
    for name, value in sorted((variables or {}).items()):
        command += f" -param:{name}={shlex.quote(str(value))}"

    finished = subprocess.run(
        ["ssh", host, command],
        capture_output=True, text=True, timeout=timeout, check=False,
    )
    log = (finished.stdout or "") + (finished.stderr or "")
    result = StepResult(
        step=step,
        artifact=str(artifact),
        exit_code=finished.returncode,
        variables=parse_variables(log),
        log=log,
    )
    if check and not result.ok:
        raise LegacyStepError(result)
    return result


def parse_variables(log: str) -> dict[str, str]:
    """The variable block from a step's log.

    A step announces what it set with one `VAR name=value` line each. A later
    line for the same name wins, which is how the vendor tools behave.
    """
    found: dict[str, str] = {}
    for line in log.splitlines():
        match = _VARIABLE_LINE.match(line.strip())
        if match:
            found[match.group(1)] = match.group(2).strip()
    return found


def _remote_path(artifact: Path) -> str:
    """Where the artifact sits on the old box.

    `legacy/` in the repository is `/opt/legacy` there, and everything under it
    keeps its layout, so `legacy/pdi/trn/load_inventory.ktr` is
    `/opt/legacy/pdi/trn/load_inventory.ktr`.
    """
    parts = artifact.parts
    tail = parts[parts.index("legacy") + 1:] if "legacy" in parts else parts
    return "/".join((LEGACY_ROOT, *tail))
