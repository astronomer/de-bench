"""The Modal side of de-bench: images, secrets, and the per-trial function.

One trial = one Modal container: extract the workspace to /work, snapshot it in git,
run the agent under the wall-clock cap, and return the end-state diff + transcript +
telemetry. No grading — the artifacts are the output.

Model access is configured by the `de-bench-llm` Modal secret, which supplies
ANTHROPIC_API_KEY and, optionally, ANTHROPIC_BASE_URL for a gateway. pi has no
ANTHROPIC_BASE_URL of its own, so a tiny pi extension points pi's anthropic provider
at the gateway and registers the gateway's OpenAI routes as a second provider. The
gpt configs need such a gateway; the claude configs work against the public API.
"""

from __future__ import annotations

import base64
import json
import os

import modal

from de_bench.agents.pi import OPENAI_MODELS, OPENAI_PROVIDER
from de_bench.tasks import repo_root

PYTHON_VERSION = "3.12"
AIRFLOW_VERSION = "3.0.3"

app = modal.App("de-bench")

llm_secret = modal.Secret.from_name("de-bench-llm")

# Mirrors the corpus repo's runtime image (docker/runtime/Dockerfile): Airflow 3 plus
# the packages the worlds rely on, with dbt/cosmos telemetry and
# caching noise switched off.
runtime_image = (
    modal.Image.debian_slim(python_version=PYTHON_VERSION)
    # procps and the rest are here because agents reach for them constantly and the
    # image did not have them: `ps` alone was not-found in 344 trials, where the agent
    # was checking whether the Airflow it started is up. Each miss costs a turn.
    .apt_install(
        "git", "curl", "ca-certificates", "ripgrep",
        "procps", "psmisc", "unzip", "sqlite3", "file", "iproute2", "jq",
    )
    .run_commands(
        "curl -fsSL https://deb.nodesource.com/setup_22.x | bash -",
        "apt-get install -y nodejs",
    )
    .pip_install(
        f"apache-airflow=={AIRFLOW_VERSION}",
        "apache-airflow-providers-standard",
        # The upgrade world's projects import these at parse time; DagBag
        # grading needs them resolvable even though their operators never
        # reach out of the container.
        "apache-airflow-providers-amazon",
        "apache-airflow-providers-cncf-kubernetes",
        "apache-airflow-providers-common-sql",
        "apache-airflow-providers-google",
        "apache-airflow-providers-http",
        "apache-airflow-providers-postgres",
        "apache-airflow-providers-snowflake",
        extra_options=(
            "--constraint https://raw.githubusercontent.com/apache/airflow/"
            f"constraints-{AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt"
        ),
    )
    .pip_install(
        "pytest",
        "pytest-timeout",
        "duckdb>=1.1",
        "dbt-duckdb>=1.8",
        "dag-factory>=0.22",
        "astronomer-cosmos==1.15.0",
        "airflow-blueprint==0.4.0",
    )
    .env(
        {
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "False",
            "DO_NOT_TRACK": "1",
            "SCARF_NO_ANALYTICS": "1",
            "AIRFLOW__COSMOS__ENABLE_TELEMETRY": "False",
            "AIRFLOW__COSMOS__ENABLE_CACHE": "0",
            "AIRFLOW__COSMOS__ENABLE_DAG_VERSIONING": "False",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        }
    )
    # Pre-init the metadata DB so `airflow` CLI calls inside a trial are fast.
    .run_commands("airflow db migrate >/dev/null 2>&1 || true")
    # uv + a managed 3.11 for live-env tasks: their virtualenvs are built at
    # trial/score time from the workspace's own requirements, and 3.11 is the
    # one interpreter every Airflow from 2.6 to 3.0 accepts.
    .pip_install("uv")
    .run_commands("uv python install 3.11")
)

# uv cache for live-env builds. Container-local on purpose: a shared Volume
# corrupts under concurrent writers (two scorers building the same sdist), and
# Modal reuses containers across map items, so the first build in a container
# pays the download and the rest in that container hardlink out of its cache.
UV_CACHE_DIR = "/tmp/uv-cache"
LIVE_VENV = "/opt/live-env"
LIVE_AIRFLOW_HOME = "/opt/live-airflow-home"


def prepare_live_env(workdir: str, start_airflow: str | None) -> dict:
    """Build the project's environment from its own requirements.

    Returns {"env": PATH/VIRTUAL_ENV/AIRFLOW_HOME overrides or None,
             "built": bool, "detail": str}. env None with built False means
    there was nothing to build (no pin anywhere) — caller keeps the image env.
    A failed install also returns env None, with the failure in detail; the
    caller decides whether that is an infra problem (trial) or the agent's
    broken future state (scoring).
    """
    import os
    import re
    import shutil
    import subprocess

    req = os.path.join(workdir, "requirements.txt")
    req_exists = os.path.exists(req)
    pin = ranged = None
    if req_exists:
        text = open(req, errors="replace").read()
        m = re.search(r"^\s*apache-airflow==([0-9][^\s#;]*)", text, re.M)
        pin = m.group(1) if m else None
        # A range pin (>=3.0,<4.0) is a legitimate spelling this builder cannot
        # anchor a constraints file to; resolve it with uv instead of silently
        # falling back to another world.
        ranged = pin is None and re.search(r"^\s*apache-airflow\s*[><~^!]", text, re.M) is not None
    version = pin or (None if ranged else start_airflow)
    if version is None and not ranged:
        return {"env": None, "built": False, "detail": "no airflow pin to build from"}

    from de_bench.scoring import progress

    shutil.rmtree(LIVE_VENV, ignore_errors=True)
    shutil.rmtree(LIVE_AIRFLOW_HOME, ignore_errors=True)

    def run(argv: list[str], timeout: int, env: dict | None = None) -> subprocess.CompletedProcess:
        run_env = dict(env or os.environ)
        run_env["UV_CACHE_DIR"] = UV_CACHE_DIR
        # cwd matters: pip resolves a requirements file's relative paths
        # (`-e .`, `-e ./common`) against the working directory, not the file's
        # own — an agent that packages shared code editable is doing the ticket,
        # not breaking the build.
        return subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout, check=False, env=run_env, cwd=workdir
        )

    progress(f"building airflow {version or 'ranged'} environment")
    steps = [(["uv", "venv", "--python", "3.11", LIVE_VENV], 120)]
    if version is not None:
        # Core first under its constraints file (the only reproducible way to
        # install an old Airflow), then the project's requirements unconstrained
        # so its own library pins (old cosmos, old providers) win.
        steps.append(
            (
                [
                    "uv", "pip", "install", "-p", f"{LIVE_VENV}/bin/python",
                    f"apache-airflow=={version}",
                    "--constraint",
                    f"https://raw.githubusercontent.com/apache/airflow/constraints-{version}/constraints-3.11.txt",
                ],
                900,
            )
        )
    if req_exists:
        steps.append((["uv", "pip", "install", "-p", f"{LIVE_VENV}/bin/python", "-r", req], 900))
    for argv, timeout in steps:
        try:
            proc = run(argv, timeout)
        except subprocess.TimeoutExpired:
            return {"env": None, "built": False, "detail": f"env build timed out: {' '.join(argv[:4])}"}
        if proc.returncode != 0:
            return {"env": None, "built": False, "detail": f"env build failed: {(proc.stderr or '')[-600:]}"}

    overrides = {
        "PATH": f"{LIVE_VENV}/bin{os.pathsep}{os.environ.get('PATH', '')}",
        "VIRTUAL_ENV": LIVE_VENV,
        "AIRFLOW_HOME": LIVE_AIRFLOW_HOME,
    }
    # A migrated metadata DB so `airflow dags test` works out of the box; 2.6
    # predates `db migrate`, hence the fallback.
    db_env = dict(os.environ) | overrides
    migrate = run([f"{LIVE_VENV}/bin/airflow", "db", "migrate"], 300, env=db_env)
    if migrate.returncode != 0:
        migrate = run([f"{LIVE_VENV}/bin/airflow", "db", "init"], 300, env=db_env)
    if migrate.returncode != 0:
        return {"env": None, "built": False, "detail": f"airflow db init failed: {(migrate.stderr or '')[-600:]}"}
    if version is None:  # ranged spec — report what uv actually resolved
        probe = run([f"{LIVE_VENV}/bin/python", "-c", "import airflow; print(airflow.__version__)"], 120)
        version = probe.stdout.strip() if probe.returncode == 0 and probe.stdout.strip() else "unresolved"
    progress(f"airflow {version} environment ready")
    return {"env": overrides, "built": True, "detail": f"airflow {version}"}

#: What the agent must be able to reach for the trial to mean anything. Recorded per
#: trial rather than assumed, because every mechanical fault this benchmark has had
#: was invisible in the stored results and took a hand audit to find: codex lost the
#: PATH additions to a login shell and never saw `af` or the task's Airflow venv, and
#: `ps` was missing from the image for 344 trials. Both are one column here.
_PREFLIGHT_TOOLS = ("af", "airflow", "python3", "git", "ps", "uv")


def _preflight(env: dict, extra: tuple[str, ...] = ()) -> dict:
    """Where the trial's tools actually resolve, from the env the agent will get.

    Resolved through a login shell as well as directly: that difference is exactly
    what bit codex, whose every command runs under `bash -lc`.
    """
    import shutil
    import subprocess

    probes = _PREFLIGHT_TOOLS + tuple(t for t in extra if t not in _PREFLIGHT_TOOLS)
    path = env.get("PATH", "")
    tools = {tool: (shutil.which(tool, path=path) or "") for tool in probes}
    login = subprocess.run(
        ["/bin/bash", "-lc", "command -v " + " ".join(probes)],
        capture_output=True, text=True, check=False, env=env,
    )
    return {
        "tools": tools,
        "path": path,
        "login_shell_tools": sorted(login.stdout.split()),
    }


# pi extension pointing pi at the LLM gateway: claude on pi's built-in anthropic
# provider, the gpt models on one registered here. The provider id and the model
# table come from agents/pi.py, which is also what picks the provider per model, so
# the two cannot drift.
#
# The holes are filled by str.replace rather than an f-string, because the TypeScript
# is full of braces that either would try to read as fields.
_GATEWAY_EXTENSION = """\
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const baseUrl = process.env.ANTHROPIC_BASE_URL;
  if (!baseUrl) {
    return;
  }
  // A trailing slash or /v1 doubles the path once pi appends /v1/messages.
  const normalized = baseUrl.replace(/\\/+$/, "").replace(/\\/v1$/, "");
  pi.registerProvider("anthropic", { baseUrl: normalized });

  // The other half of the same gateway. Note the /v1 the anthropic override strips:
  // pi appends the whole path (/v1/messages) for anthropic and only the tail
  // (/responses) for an OpenAI-shaped provider, so the two want different roots. The
  // gateway takes the same token either way, as a bearer header here.
  pi.registerProvider(__PROVIDER__, {
    name: "LLM gateway (OpenAI)",
    baseUrl: `${normalized}/v1`,
    apiKey: "$ANTHROPIC_API_KEY",
    api: "openai-responses",
    authHeader: true,
    models: __MODELS__,
  });
}
"""
_GATEWAY_EXTENSION = _GATEWAY_EXTENSION.replace(
    "__PROVIDER__", json.dumps(OPENAI_PROVIDER)
).replace("__MODELS__", json.dumps([dict(model) for model in OPENAI_MODELS], indent=2))
_ext_b64 = base64.b64encode(_GATEWAY_EXTENSION.encode()).decode()

# pi, pinned the way codex and opencode are. The trial cache doesn't key on the
# image, so an unpinned install meant a rebuild could put fresh cells on a newer pi
# than the cached ones beside them. 0.84.1 is what every run on record used.
PI_VERSION = "0.84.1"

#: Claude Code, pinned to what every matrix on record ran. See the install below.
CLAUDE_CODE_VERSION = "2.1.224"

#: What each harness's `--version` must contain. Checked per trial, because a pin in
#: the image is only a claim until something reads it back: claude-code was the one
#: harness left unpinned, a rebuild moved it two patch versions, and the new one loads
#: bundled skills into the prompt — 700KB of one ended four haiku trials. Nothing in
#: the results said so. This turns the next drift into a line in every summary.
EXPECTED_VERSIONS = {
    "pi": PI_VERSION,
    "claude-code": CLAUDE_CODE_VERSION,
    "codex": "0.146.1",
    "opencode": "1.18.14",
}

# The agent toolchain layers, shared by every trial image. Local source is
# added per-image below — Modal wants non-copy local layers last, and the
# copperline image continues the chain with layers of its own.
_agent_base = (
    runtime_image.run_commands(
        f"npm install -g --ignore-scripts @earendil-works/pi-coding-agent@{PI_VERSION}",
        "mkdir -p /root/.pi/agent/extensions",
        f"echo {_ext_b64} | base64 -d > /root/.pi/agent/extensions/gateway.ts",
        "pi --version",
    )
    # claude-code: Anthropic's Claude Code CLI. It honors ANTHROPIC_BASE_URL and
    # ANTHROPIC_API_KEY from the secret natively, so unlike pi it needs no
    # extension — just the install. Per-run env (IS_SANDBOX etc.) is set in the
    # adapter's pre_run.
    #
    # Pinned, like every other harness here. Unpinned, a rebuild moved it from
    # 2.1.224 to 2.1.226, which bundles skills and loads them into the prompt: on an
    # Airflow upgrade task an agent pulled in `claude-api`, 700KB about building LLM
    # applications, and haiku's 200k window ended the trial with "Prompt is too long"
    # 30 seconds in. Four trials died that way in the first 90 of a sweep.
    .run_commands(
        f"npm install -g @anthropic-ai/claude-code@{CLAUDE_CODE_VERSION}",
        "claude --version",
    )
    # codex (OpenAI Codex CLI) for the codex adapter — same shared trial image;
    # its gateway config is written per-trial by the adapter's pre_run.
    # Pinned: 0.147.0 sends a tool the gateway rejects ("tools[0].description:
    # empty string"), which zeroed all three codex columns of a matrix run.
    # 0.146.1 is the last version with working runs on record.
    .run_commands(
        "npm install -g @openai/codex@0.146.1",
        "codex --version",
    )
    # opencode CLI (opencode adapter). Pinned: the adapter's config, flags, and
    # telemetry parsing were verified against this version. Gateway wiring happens
    # at trial time — the adapter's pre_run writes ~/.config/opencode/opencode.json.
    .run_commands(
        "npm install -g opencode-ai@1.18.14",
        "opencode --version",
    )
)
pi_image = _agent_base.add_local_python_source("de_bench")

WORKDIR = "/work"

# ---------------------------------------------------------------------------
# The copperline world's image: the shared agent image moved to the world's
# own pins, with the generated warehouse and landing tree baked at build.
#
# Two environments on purpose, and it is probed fact, not preference:
# apache-airflow 3.3.1 and dbt-core 1.6.14 cannot share an environment
# (dbt-semantic-interfaces <=0.2.3 pins importlib-metadata <7; airflow-core
# 3.3.1 needs newer). The Airflow env runs 3.3.1 on the image Python; dbt
# lives in /opt/dbt-venv on 3.11, and the world invokes /opt/dbt-venv/bin/dbt
# — which is what real cosmos deployments do.
#
# The bake embeds the repo config's generated_base_sha in the build command,
# so editing timeline.yaml or the generator rebuilds these layers, and writes
# it beside the artifacts, so setup can refuse an image that does not match
# the config the cache key was computed from.
# ---------------------------------------------------------------------------
COPPERLINE_AIRFLOW_VERSION = "3.3.1"
COPPERLINE_DATA_DIR = "/opt/copperline"
COPPERLINE_DBT_VENV = "/opt/dbt-venv"
COPPERLINE_BAKE_SHA_PATH = f"{COPPERLINE_DATA_DIR}/BAKE_SHA"


COPPERLINE_FIXTURES = "worlds/copperline/workspace/fixtures"
COPPERLINE_FIXTURES_DIR = "/opt/gen-fixtures"


def _copperline_bake_sha() -> str:
    """The config sha the bake stamps. Meaningful only where the repo exists:
    this module also imports inside containers, which carry the harness source
    but not worlds/, so remotely the literal is inert — the image was already
    built from the client's value by then."""
    if not modal.is_local():
        return "remote-import"
    from de_bench.tasks import generated_base_sha

    return generated_base_sha("copperline") or "unbaked"


def _copperline_fixtures_sha() -> str:
    """Content hash of the hand-authored fixtures the bake lands into `raw`,
    and of the loader that lands them.

    They ship as source under `workspace/`, so `world_sha` already folds them
    into every trial cache key. The bake does not see them that way — the
    generator neither reads nor writes them — so this hash goes into the build
    command as well, and editing a fixture rebuilds the layer that loads it.

    The loader is hashed alongside them because it does more than copy now: it
    also creates the staging views a check has to be able to read (finding 018).
    Those live only in the baked warehouse, so an edit to `STAGING_VIEWS` that
    did not bust this layer would leave every trial reading the old view.
    """
    if not modal.is_local():
        return "remote-import"
    import hashlib

    from de_bench.tasks import _tree_sha, repo_root

    tree = _tree_sha(repo_root() / COPPERLINE_FIXTURES)
    loader = (repo_root() / "tools/gen_copperline_load_fixtures.py").read_bytes()
    return hashlib.sha256(tree.encode() + loader).hexdigest()[:16]


copperline_image = (
    _agent_base
    .pip_install(
        f"apache-airflow=={COPPERLINE_AIRFLOW_VERSION}",
        "apache-airflow-providers-standard",
        extra_options=(
            "--constraint https://raw.githubusercontent.com/apache/airflow/"
            f"constraints-{COPPERLINE_AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt"
        ),
    )
    .pip_install("astronomer-cosmos==1.14.0", "duckdb==1.5.5", "pyyaml")
    .run_commands(
        # The dedicated dbt environment (see the header note).
        f"uv venv --python 3.11 {COPPERLINE_DBT_VENV}",
        f"uv pip install -p {COPPERLINE_DBT_VENV}/bin/python "
        "dbt-core==1.6.14 dbt-duckdb==1.6.2 duckdb==1.5.5",
        f"{COPPERLINE_DBT_VENV}/bin/dbt --version",
        # The duckdb CLI at the warehouse's own version.
        "curl -fsSL --retry 3 -o /tmp/duckdb_cli.zip "
        "https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-amd64.zip",
        "unzip -o /tmp/duckdb_cli.zip -d /usr/local/bin && rm /tmp/duckdb_cli.zip && duckdb --version",
        # Re-migrate the pre-initialized metadata DB template at 3.3.1.
        "airflow db migrate >/dev/null 2>&1 || true",
    )
    .add_local_dir("tools/gen_copperline", "/opt/gen/gen_copperline", copy=True)
    .add_local_file("worlds/copperline/timeline.yaml", "/opt/gen/timeline.yaml", copy=True)
    .add_local_file("worlds/copperline/planted.yaml", "/opt/gen/planted.yaml", copy=True)
    # The hand-authored fixtures and the loader that lands them. Extract
    # convention 7 keeps these out of the generator, so they arrive as their
    # own layer and land after the bake.
    .add_local_dir(COPPERLINE_FIXTURES, COPPERLINE_FIXTURES_DIR, copy=True)
    .add_local_file("tools/gen_copperline_load_fixtures.py",
                    "/opt/gen/load_fixtures.py", copy=True)
    # dbt package install needs network, which a trial never has; resolve at
    # build and ship the result via workspace_data like the warehouse.
    .add_local_file("worlds/copperline/workspace/dbt/copperline_analytics/dbt_project.yml",
                    "/opt/gen/depsproj/dbt_project.yml", copy=True)
    .add_local_file("worlds/copperline/workspace/dbt/copperline_analytics/packages.yml",
                    "/opt/gen/depsproj/packages.yml", copy=True)
    .run_commands(
        f"cd /opt/gen/depsproj && {COPPERLINE_DBT_VENV}/bin/dbt deps --profiles-dir .",
        f"mkdir -p {COPPERLINE_DATA_DIR} && "
        f"mv /opt/gen/depsproj/dbt_packages {COPPERLINE_DATA_DIR}/dbt_packages",
    )
    .run_commands(
        # The bake. The sha literal below is computed from the repo config at
        # image-definition time; it doubles as the cache-buster.
        "cd /opt/gen && PYTHONPATH=/opt/gen python -m gen_copperline "
        "--timeline timeline.yaml --planted planted.yaml "
        f"--db {COPPERLINE_DATA_DIR}/copperline.duckdb "
        f"--landing {COPPERLINE_DATA_DIR}/landing --profile full",
        # The hand-authored half, after the generated half: the reference
        # tables into `raw`, and the finance workbook into the landing tree
        # `plat_workbook_inbox` globs. The fixtures sha in the comment is the
        # cache-buster for this step — editing a fixture rebuilds from here.
        f"cd /opt/gen && python load_fixtures.py "
        f"--fixtures {COPPERLINE_FIXTURES_DIR} "
        f"--db {COPPERLINE_DATA_DIR}/copperline.duckdb "
        f"--landing {COPPERLINE_DATA_DIR}/landing "
        f"# fixtures {_copperline_fixtures_sha()}",
        f"echo {_copperline_bake_sha()} > {COPPERLINE_BAKE_SHA_PATH}",
        # The generator and its config are the answer key; they do not stay
        # in the image the agent runs in. The fixtures ship as source, so the
        # copy the loader read goes too.
        f"rm -rf /opt/gen {COPPERLINE_FIXTURES_DIR}",
    )
)


# Retries absorb worker preemption: three whole-sweep runs each lost a wave
# of cells 45-55 minutes in to "cancelled by user or a failure", the
# signature of preempted containers with no retry budget. A retried trial is
# a fresh sample of the same cell — duplicate model spend only for the
# preempted few.
_TRIAL_RETRIES = modal.Retries(max_retries=2, initial_delay=10.0)

#: Concurrent trial containers, per function. Uncapped, a full-matrix sweep
#: queues thousands of trials and Modal scales to hundreds of simultaneous
#: agent sessions — which the LLM gateway answers with rate limits, and a
#: rate-limited trial is an infra_failed husk to resample, not a sample.
#: 64 keeps a big sweep moving without the storm.
_TRIAL_CONCURRENCY = 64


@app.function(image=pi_image, secrets=[llm_secret], timeout=5400, cpu=2.0, memory=4096,
              retries=_TRIAL_RETRIES, max_containers=_TRIAL_CONCURRENCY)
def run_trial(spec: dict) -> dict:
    return _run_trial(spec)


@app.function(image=copperline_image, secrets=[llm_secret], timeout=5400, cpu=2.0,
              memory=8192, retries=_TRIAL_RETRIES, max_containers=_TRIAL_CONCURRENCY)
def run_trial_copperline(spec: dict) -> dict:
    return _run_trial(spec)


# The generator's whole command line. The generator is answer-key code that never
# ships inside a world, so the contract it implements is written down here, on the
# harness side:
#
#   python -m <pkgname> --timeline timeline.yaml --planted planted.yaml \
#           --db <DUCKDB_PATH> --profile shipped
#
# run with cwd set to the staging directory, that directory first on PYTHONPATH,
# and the trial's own environment on top of the container's — so a generator that
# reads WORLD_TODAY sees the same value the agent will.
GENERATOR_PROFILE = "full"

# DuckDB 1.5 has no setting that pins the wall clock, but a macro stored in the
# database file shadows the built-in of the same name, and later independent
# sessions against that file resolve to the macro — dbt-duckdb's sessions
# included. So the pin lives in the warehouse, and `current_date - 7` in shipped
# world SQL means seven days before WORLD_TODAY however long the world sits.
#
# Midday, not midnight: a timestamp cast back to a date under a container
# timezone west or east of UTC would otherwise land on the day either side.
_CLOCK_MACROS = (
    ("current_date", "DATE '{d}'"),
    ("today", "DATE '{d}'"),
    ("now", "TIMESTAMPTZ '{d} 12:00:00+00'"),
    ("current_timestamp", "TIMESTAMPTZ '{d} 12:00:00+00'"),
    ("get_current_timestamp", "TIMESTAMPTZ '{d} 12:00:00+00'"),
    ("transaction_timestamp", "TIMESTAMPTZ '{d} 12:00:00+00'"),
    ("current_localtimestamp", "TIMESTAMP '{d} 12:00:00'"),
    ("current_localtime", "TIME '12:00:00'"),
)


def _run_generator(workdir: str, spec: dict) -> None:
    """Run the world's data generator into the trial's DuckDB file.

    The tar unpacks to a staging directory outside the workdir and is deleted
    the moment the generator returns, so nothing the generator carries can reach
    the workspace, the git baseline or the end-state diff. A generator that fails
    fails the setup: the data the tasks are graded against is not optional.
    """
    import io
    import os
    import shutil
    import subprocess
    import sys
    import tarfile
    import tempfile

    from de_bench.scoring import progress

    db_path = dict(spec.get("env") or {}).get("DUCKDB_PATH", "")
    if not db_path:
        raise RuntimeError("generator: the world sets no DUCKDB_PATH, so there is nowhere to write")
    # The generator runs with the staging dir as its cwd, so a workdir-relative
    # path must be resolved here or the warehouse lands in staging and dies with it.
    db_path = os.path.join(workdir, db_path)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    staging = tempfile.mkdtemp(prefix="de-bench-generator-")
    try:
        with tarfile.open(fileobj=io.BytesIO(spec["generated_tar"]), mode="r:gz") as tar:
            tar.extractall(staging, filter="data")
        packages = [n for n in sorted(os.listdir(staging)) if os.path.isdir(os.path.join(staging, n))]
        if len(packages) != 1:
            raise RuntimeError(f"generator: expected one generator package in the tar, found {packages}")
        package = packages[0]

        env = dict(os.environ)
        env.update({str(k): str(v) for k, v in dict(spec.get("env") or {}).items()})
        env["PYTHONPATH"] = os.pathsep.join(p for p in (staging, env.get("PYTHONPATH", "")) if p)
        progress(f"generator: {package} -> {db_path}")
        proc = subprocess.run(
            [
                sys.executable, "-m", package,
                "--timeline", "timeline.yaml",
                "--planted", "planted.yaml",
                "--db", db_path,
                # The dated landing files (landing/<source>/dt=<ds>/...) land
                # inside the workspace so the world's ingest DAGs can read
                # them; the git baseline excludes landing/** the way it
                # excludes *.duckdb, so none of it enters the end-state diff.
                "--landing", os.path.join(workdir, "landing"),
                "--profile", GENERATOR_PROFILE,
            ],
            cwd=staging, env=env, capture_output=True, text=True, check=False,
        )
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "")[-2000:]
            progress(f"generator FAILED: {tail}")
            raise RuntimeError(f"generator: {package} exited {proc.returncode}: {tail}")
        progress("generator: wrote the warehouse")
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _pin_warehouse_clock(workdir: str, spec: dict) -> None:
    """Make `current_date` in the warehouse mean WORLD_TODAY, in every later session."""
    import datetime as dt
    import os

    env = dict(spec.get("env") or {})
    today, db_path = env.get("WORLD_TODAY"), env.get("DUCKDB_PATH")
    if not (today and db_path):
        return
    db_path = os.path.join(workdir, db_path)
    # Straight into SQL text, so refuse anything that is not a plain date.
    today = dt.date.fromisoformat(str(today)).isoformat()

    import duckdb

    from de_bench.scoring import progress

    con = duckdb.connect(db_path)
    try:
        for name, value in _CLOCK_MACROS:
            con.execute(f"CREATE OR REPLACE MACRO {name}() AS {value.format(d=today)}")
    finally:
        con.close()
    progress(f"clock: warehouse current_date pinned to {today}")


def _prepare_workspace_extras(workdir: str, spec: dict) -> None:
    """Land image-carried data, generated data and per-task setup SQL.

    Runs in both the trial and the scorer, before anything reads the tree, so
    the two see the same world. Order is load-bearing: the generator first, then the
    clock, then setup SQL — a task's own SQL builds on the generated tables and
    reads the pinned date.
    """
    import os
    import shutil
    import subprocess

    if spec.get("baked_sha"):
        # The image carries the generated world; refuse one whose bake does
        # not match the config this trial's cache key was computed from.
        baked = ""
        if os.path.exists(COPPERLINE_BAKE_SHA_PATH):
            baked = open(COPPERLINE_BAKE_SHA_PATH).read().strip()
        if baked != spec["baked_sha"]:
            raise RuntimeError(
                f"baked world {baked or 'missing'} does not match the config "
                f"{spec['baked_sha']} — the image needs a rebuild")
    for src, rel in spec.get("workspace_data") or ():
        dest = os.path.join(workdir, rel)
        if os.path.isdir(src):
            # A directory ships as a tree — the landing zone is thousands of
            # dated files, not one.
            shutil.copytree(src, dest, dirs_exist_ok=True)
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copyfile(src, dest)
    if spec.get("generated_tar"):
        _run_generator(workdir, spec)
    _pin_warehouse_clock(workdir, spec)
    for rel in spec.get("setup_sql") or ():
        from de_bench.scoring import progress

        sql_path = os.path.join(workdir, rel)
        db_path = dict(spec.get("env") or {}).get("DUCKDB_PATH", "")
        progress(f"setup sql: {rel}")
        code = (
            "import duckdb,sys\n"
            f"con = duckdb.connect({db_path!r})\n"
            f"con.execute(open({sql_path!r}).read())\n"
            "con.close()\n"
        )
        proc = subprocess.run(["python", "-c", code], capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            progress(f"setup sql FAILED: {rel}: {(proc.stderr or '')[-300:]}")


def _run_trial(spec: dict) -> dict:
    """Run one agent on one task and return artifacts + telemetry.

    spec: agent, task_id, prompt, system_prompt, model, thinking,
          max_wall_seconds, workspace_tar (gzipped tar bytes)
    """
    import io
    import os
    import subprocess
    import tarfile
    import threading
    import time

    from de_bench.agents import AGENTS
    from de_bench.scoring import _PROGRESS, progress

    agent = AGENTS[spec["agent"]]

    # Every line this trial prints carries its cell, so the Modal log stays
    # readable while many trials interleave.
    _PROGRESS["label"] = f"{spec.get('config') or spec['agent']}/{spec['task_id']}/t{spec.get('trial', 0)}"
    progress(f"start: {spec['agent']} on {spec['model']}, thinking {spec['thinking']}")

    # Modal reuses containers across map items — wipe every cross-trial surface.
    import shutil

    shutil.rmtree(WORKDIR, ignore_errors=True)
    if agent.session_dir:
        shutil.rmtree(agent.session_dir, ignore_errors=True)
    os.makedirs(WORKDIR, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(spec["workspace_tar"]), mode="r:gz") as tar:
        tar.extractall(WORKDIR)
    _prepare_workspace_extras(WORKDIR, spec)

    def git(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                "git",
                "-C",
                WORKDIR,
                "-c",
                # A plausible bootstrap identity. Six agents ran `git log`,
                # saw the old "de-bench <bench@de-bench>" author, and learned
                # they were inside a benchmark. World-neutral on purpose: the
                # same helper commits every world's baseline.
                "user.email=platform@internal",
                "-c",
                "user.name=platform",
                "-c",
                f"safe.directory={WORKDIR}",
                *args,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    # Run-state the world regenerates (warehouse files, dbt target/, logs) is not part
    # of what the agent "left behind" — keep it out of both the baseline and the diff.
    #
    # .astro/ is the Astro CLI's own directory. We put the project marker there so the
    # CLI recognises the project; everything else in it is the CLI's state, and
    # `astro dev start --standalone` fills it with an Airflow database, a pid file,
    # logs and constraint sets — 184KB of runtime in one trial's diff. No task grades
    # anything under it. The files still sit on disk for the CLI to read; they just
    # aren't the agent's work.
    _add_excludes = (
        "--",
        ".",
        ":(exclude,glob)**/*.duckdb",
        ":(exclude,glob)**/target/**",
        ":(exclude,glob)**/logs/**",
        ":(exclude,glob)**/__pycache__/**",
        ":(exclude,glob)**/.astro/**",
        # dbt package installs are environment bootstrap, not the agent's
        # work. Scoring's prepare runs deps itself, so a package the agent
        # added still reaches the verifier.
        ":(exclude,glob)**/dbt_packages/**",
        # Generated landing files (landing/<source>/dt=<ds>/...) are world
        # data the generator writes at setup, like the .duckdb warehouse —
        # hundreds of MB that are never the agent's work.
        ":(exclude,glob)**/landing/**",
        # Failure-callback notification lines are the world's own runtime
        # bookkeeping (include/lib/notify.py); a DAG failing during a trial
        # must not put them in the agent's diff.
        ":(exclude,glob)**/_notifications/**",
        # Partition files DAGs publish under include/data/ are run output,
        # not the agent's work. An agent that runs a DAG to look at the data
        # must not carry the published files into its patch — scoring's own
        # replay runs regenerate every partition a check grades, and a
        # leftover from before the agent's fix would fail file_absent checks
        # the fix itself satisfies.
        ":(exclude,glob)**/include/data/**",
        # dbt writes its manifest wherever a project pins manifest-path; a
        # `dbt run` during a trial regenerates it and one 4.4MB rewrite was
        # 99.7% of a stored patch. Run output, not the agent's work.
        ":(exclude,glob)**/manifest/manifest.json",
    )

    git("init", "-q")
    git("add", "-A", *_add_excludes)
    git("commit", "-q", "-m", "initial import")
    # The diff at the end is taken against this SHA, not against HEAD: an agent that
    # commits its own work moves HEAD, and a diff against HEAD then comes back empty
    # — the trial reads as "delivered nothing" having done the whole task. That cost
    # 13 trials before it was caught, 10 of them one config.
    baseline = git("rev-parse", "HEAD").stdout.strip()

    from de_bench.scoring import dags_root

    env = dict(os.environ)
    # dags_root, not a hardcoded projects/: own-workspace tasks keep dags/ at
    # the root, and pointing the agent's own `airflow` CLI at a folder that
    # does not exist quietly took `dags test` away from every upgrade trial.
    env["AIRFLOW__CORE__DAGS_FOLDER"] = dags_root(WORKDIR)
    # `include/` sits at the workspace root and the DAGs under projects/<team>/dags
    # import it as a package. Scoring has always put the root on PYTHONPATH; the
    # trial did not, so an agent running `airflow dags test` to check its own work
    # depended entirely on each DAG splicing sys.path. Set it here too — and the
    # dags root beside it, matching Airflow 3's dag processor, which appends the
    # bundle root to sys.path (sibling imports inside dags/ are production-legal).
    _pp = f"{WORKDIR}{os.pathsep}{dags_root(WORKDIR)}"
    # And plugins/, when the project ships one — Airflow's settings.py appends
    # PLUGINS_FOLDER to sys.path, so bare imports of plugins modules are
    # production-legal.
    if os.path.isdir(f"{WORKDIR}/plugins"):
        env["AIRFLOW__CORE__PLUGINS_FOLDER"] = f"{WORKDIR}/plugins"
        _pp += os.pathsep + f"{WORKDIR}/plugins"
    env["PYTHONPATH"] = _pp + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.setdefault("HOME", "/root")
    env.update(dict(spec.get("env") or {}))

    # A live-env task starts in the project's own world: the venv built from
    # its requirements is what `python` and `airflow` mean to the agent. If the
    # build fails the trial still runs on the image env — but the corpus tests
    # and `de-bench prove` exist to keep a shipped task from ever hitting that.
    live_note = ""
    if spec.get("live_env"):
        build = prepare_live_env(WORKDIR, spec.get("start_airflow"))
        if build["env"] is None:
            live_note = f"[harness] start env unavailable, trial ran on the image env: {build['detail']}\n"
            progress(live_note.strip())
        else:
            env.update(build["env"])

    if agent.pre_run is not None:
        agent.pre_run(env)

    # A diagnosis task starts from run history: write the seeded DagRuns into a
    # metadata database of this trial's own before the agent gets the keys. A
    # fresh AIRFLOW_HOME, not the container default — a warm container carries
    # the previous trial's runs, and history that varies by scheduling order
    # would grade luck. Runs after the live-env build so the seeds execute on
    # the same Airflow the agent will use. write_history is the writer the
    # scorer calls too, so the grader reads the history the agent read.
    if spec.get("history"):
        from de_bench.scoring import write_history

        af_home = "/tmp/de-bench-af-home"
        shutil.rmtree(af_home, ignore_errors=True)
        env["AIRFLOW_HOME"] = af_home
        write_history(env, WORKDIR, spec["history"], migrate=True)

    # Worlds can extend the probe set — the default list is Airflow-shaped,
    # and a dbt-only world needs duckdb and dbt checked too.
    preflight = _preflight(env, extra=tuple(spec.get("preflight_tools") or ()))
    absent = [tool for tool, path in preflight["tools"].items() if not path]
    if absent:
        progress(f"[harness] not on the agent's PATH: {', '.join(absent)}")

    argv = agent.build_argv(spec["prompt"], spec["system_prompt"], spec["model"], spec["thinking"])
    cap = int(spec["max_wall_seconds"])

    start = time.monotonic()
    exit_reason, exit_code = "completed", 0
    stdout, stderr = "", ""

    # Read the agent's stream as it arrives so the work is watchable, while still
    # keeping every raw byte — the transcript artifact is this stdout verbatim.
    from de_bench.live import summarize

    # That was the last de_bench import this function makes, and Python keeps
    # loaded modules in memory — so take the source off the disk the agent can
    # browse. It ships into the container only so this function can run, but an
    # agent that finds it can read the grading mechanism (one did, mid-run, and
    # said so in its transcript). Warm containers stay clean and working: files
    # gone, modules cached.
    import de_bench

    _src_dir = os.path.dirname(de_bench.__file__)
    shutil.rmtree(_src_dir, ignore_errors=True)
    if os.path.exists(_src_dir):
        progress("WARNING: harness source could not be removed from the container")

    progress(f"agent started, {cap}s cap")
    # Own session so the wall cap can kill the whole process group: harnesses
    # spawn children that inherit the output pipe, and killing only the parent
    # leaves them holding the stream open — one capped opencode trial ran
    # 2257s on a 900s cap, and a second wedged an entire run's exit.
    proc = subprocess.Popen(
        argv, cwd=WORKDIR, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        start_new_session=True,
    )

    out_lines: list[str] = []
    err_lines: list[str] = []
    timed_out = threading.Event()

    def _drain_stderr() -> None:
        # Left unread, a full stderr pipe deadlocks the agent mid-run.
        for line in proc.stderr:
            err_lines.append(line)

    def _give_up() -> None:
        # SIGTERM first, then SIGKILL five seconds later. Harnesses write their
        # session file when the turn ends, so killing outright took the trace with
        # it: every one of opencode's 17 capped trials lost session.jsonl and its
        # trajectory, which left the trials most worth reading as the only ones that
        # could not be read. The group still dies; it just gets a moment to flush.
        import signal

        timed_out.set()
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(proc.pid, sig)
            except (ProcessLookupError, PermissionError):
                try:
                    proc.kill()
                except Exception:  # noqa: BLE001
                    pass
                return
            if sig is signal.SIGTERM and proc.poll() is None:
                time.sleep(5)
            if proc.poll() is not None:
                return

    def _cut_pipes() -> None:
        # Last resort for a double-forked survivor outside the process group
        # that still holds our pipe: closing our end unblocks the read loop.
        try:
            proc.stdout.close()
        except Exception:  # noqa: BLE001
            pass

    threading.Thread(target=_drain_stderr, daemon=True).start()
    timer = threading.Timer(cap, _give_up)
    timer.start()
    breaker = threading.Timer(cap + 60, _cut_pipes)
    breaker.start()

    shown, last, LIMIT = 0, "", 400
    try:
        try:
            for line in proc.stdout:
                out_lines.append(line)
                for text in summarize(line):
                    # pi re-emits a tool call as it settles; the repeat says nothing.
                    if text == last:
                        continue
                    last = text
                    shown += 1
                    if shown <= LIMIT:
                        progress(text)
                    elif shown == LIMIT + 1:
                        progress(f"… quiet from here; the full stream is in transcript.jsonl")
        except (ValueError, OSError):
            pass  # _cut_pipes closed the stream under us — the cap already ruled
        proc.wait()
    finally:
        timer.cancel()
        breaker.cancel()

    stdout, stderr = "".join(out_lines), live_note + "".join(err_lines)
    exit_code = proc.returncode or 0
    if timed_out.is_set():
        exit_reason, exit_code = "wall_cap", -1
    elif exit_code != 0:
        exit_reason = "agent_error"
    wall_seconds = round(time.monotonic() - start, 1)
    progress(f"agent {exit_reason} in {wall_seconds}s")

    # An agent that stashes to run a differential check and hits the wall
    # mid-verification leaves its whole fix in the stash — one trial scored
    # "delivered nothing" over thirty minutes of verified work this way. The
    # stash is the agent's work product like the tree is; restore it before
    # the diff. `git stash apply` keeps the entry, and a conflict (the agent
    # re-edited the file after stashing) leaves the tree's version in place,
    # which is the newer of the two.
    stashes = git("stash", "list").stdout.strip()
    if stashes:
        progress(f"restoring {len(stashes.splitlines())} stash entr(ies) left behind")
        for _ in stashes.splitlines():
            if git("stash", "apply", "-q").returncode != 0:
                # In a stash-apply merge, --ours is the tree the agent last
                # worked in — the newer state wins over what it set aside.
                conflicted = git("diff", "--name-only", "--diff-filter=U").stdout.split()
                for path in conflicted:
                    git("checkout", "--ours", "--", path)
            git("stash", "drop", "-q")

    git("add", "-A", *_add_excludes)
    # The patch is captured as BYTES. text=True decodes with universal
    # newlines, which folds \r\n to \n — and a CRLF context line with its CR
    # stripped can never re-apply against the CRLF file it came from, so a
    # world that ships CRLF files scores apply_failed unless this is binary.
    patch = subprocess.run(
        ["git", "-C", WORKDIR, "diff", "--cached", "--binary", baseline],
        check=False, capture_output=True,
    ).stdout
    deleted = git("diff", "--cached", "--name-only", "--diff-filter=D", baseline).stdout
    stats = git("diff", "--cached", "--stat", baseline).stdout
    # Whether the agent committed is worth knowing on its own: it is the difference
    # between "wrote nothing" and "wrote it and buried it", and the two used to look
    # identical from here.
    committed = git("rev-list", "--count", f"{baseline}..HEAD").stdout.strip()

    telemetry = agent.parse_telemetry(stdout)
    changed = len([l for l in stats.splitlines() if "|" in l])
    progress(
        f"left {len(patch)} byte patch across {changed} file(s); "
        f"{telemetry.turns} turns, {telemetry.input_tokens}/{telemetry.output_tokens} tokens, "
        f"${telemetry.cost_usd:.4f}"
    )

    # The harness's own session file is the canonical, lossless trace (renderable by
    # HF's trace viewer, normalizable by trajectory). Grab the newest one if written —
    # recursive, because some harnesses nest sessions under per-project directories.
    session = b""
    if agent.session_dir and os.path.isdir(agent.session_dir):
        candidates = [
            os.path.join(root, f)
            for root, _, files in os.walk(agent.session_dir)
            for f in files
            if f.endswith(".jsonl")
        ]
        if candidates:
            newest = max(candidates, key=os.path.getmtime)
            with open(newest, "rb") as f:
                session = f.read()

    agent_version = ""
    if agent.version_argv:
        v = subprocess.run(list(agent.version_argv), capture_output=True, text=True, check=False)
        agent_version = (v.stdout or v.stderr).strip().splitlines()[0] if (v.stdout or v.stderr).strip() else ""

    expected = EXPECTED_VERSIONS.get(agent.name)
    preflight["version_expected"] = expected or ""
    preflight["version_ok"] = bool(expected) and expected in agent_version
    if expected and not preflight["version_ok"]:
        progress(f"WARNING: {agent.name} reports {agent_version!r}, image pins {expected!r} "
                 "— this trial is not comparable with the rest of the run")

    return {
        "task_id": spec["task_id"],
        "config": spec.get("config") or spec["agent"],
        "agent": spec["agent"],
        "trial": spec.get("trial", 0),
        "agent_version": agent_version,
        "model": spec["model"],
        "thinking": spec["thinking"],
        "status": exit_reason,
        "exit_code": exit_code,
        "wall_seconds": wall_seconds,
        "telemetry": telemetry.as_dict(),
        "diff_stat": stats,
        "agent_commits": int(committed or 0),
        "preflight": preflight,
        "artifacts": {
            "transcript.jsonl": stdout.encode(),
            "session.jsonl": session,
            "stderr.txt": stderr.encode(),
            "changes.patch": patch,
            "deleted.txt": deleted.encode(),
        },
    }


#: Scoring is bounded work — apply a patch, run the declared checks — not an
#: open-ended agent turn. Measured at 129-164s for copperline's heaviest task
#: (three real DAG runs plus a from-scratch pytest verifier plus a judge
#: call): 1200s gives wide headroom and still fails a stuck trial fast
#: instead of tying up a container for up to an hour on the old run-sized
#: timeouts.
SCORE_TIMEOUT = 1600


@app.function(image=pi_image, timeout=SCORE_TIMEOUT, cpu=2.0, memory=4096, secrets=[llm_secret])
def score_trial(spec: dict) -> dict:
    return _score_trial(spec)


@app.function(image=copperline_image, timeout=SCORE_TIMEOUT, cpu=2.0, memory=8192, secrets=[llm_secret])
def score_trial_copperline(spec: dict) -> dict:
    return _score_trial(spec)


def _score_trial(spec: dict) -> dict:
    """Score one stored trial: rebuild workspace, apply its patch, run the checks.

    No agent, no secrets, no model spend — pure execution of stored artifacts.
    spec: config, task_id, trial, workspace_tar, patch (bytes), checks, do_not_modify
    """
    import io
    import os
    import subprocess
    import tarfile

    from de_bench.scoring import _PROGRESS, dags_root, list_dags, progress, score_workspace

    import shutil
    import time

    # Every line this trial prints carries its cell, so the Modal log stays
    # readable while many trials interleave.
    _PROGRESS["label"] = f"{spec['config']}/{spec['task_id']}/t{spec['trial']}"
    started = time.monotonic()
    progress("start: rebuilding workspace")

    # Score at /work, same path as live trials: the world's dbt profile defaults the
    # warehouse to /work/include/data/lodestone.duckdb, so any other path starves dbt.
    workdir = "/work"

    def lay_workspace() -> None:
        """The tree as shipped: same tar, same extras, no patch. Called twice
        when a unchanged check runs the untouched tree first, so both arms
        start from the same ground."""
        shutil.rmtree(workdir, ignore_errors=True)
        os.makedirs(workdir, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(spec["workspace_tar"]), mode="r:gz") as tar:
            tar.extractall(workdir)
        _prepare_workspace_extras(workdir, spec)

    lay_workspace()

    verifier_dir = None
    if spec.get("verifier_tar"):
        verifier_dir = "/verifier"
        shutil.rmtree(verifier_dir, ignore_errors=True)
        os.makedirs(verifier_dir, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(spec["verifier_tar"]), mode="r:gz") as tar:
            tar.extractall(verifier_dir)

    oracle_dir = None
    if spec.get("oracle_tar"):
        oracle_dir = "/oracle"
        shutil.rmtree(oracle_dir, ignore_errors=True)
        os.makedirs(oracle_dir, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(spec["oracle_tar"]), mode="r:gz") as tar:
            tar.extractall(oracle_dir)

    env = dict(os.environ)
    env["AIRFLOW__CORE__DAGS_FOLDER"] = dags_root(workdir)
    env.update(dict(spec.get("env") or {}))
    dag_kinds = ("dag_parses", "dag_runs", "dag_fails", "dag_structure", "no_dag", "idempotent",
                 "unchanged")
    if any(c.get("kind") in dag_kinds for c in spec["checks"]):
        progress("reading the world's baseline DAGs")
        baseline_dags = list_dags(workdir, env)
    else:
        baseline_dags = []  # nothing DAG-shaped to grade, skip the DagBag read

    # Differential grading: the untouched world is the expected result, so it
    # runs first — here, in this container, on the tree the tar just laid, with
    # the environment the world ships. A live_env task's patched arm may later
    # run on a venv built from the patched pins; the original arm never should,
    # because the point of the comparison is what this world produced BEFORE the
    # agent touched it. The tree is then laid again so the patch applies to the
    # same ground the original arm ran on.
    original_dir = None
    if any(c.get("kind") == "unchanged" for c in spec["checks"]):
        from de_bench.scoring import build_score_env, capture_relations, unchanged_plan

        original_dir = "/original"
        shutil.rmtree(original_dir, ignore_errors=True)
        os.makedirs(original_dir, exist_ok=True)
        progress("unchanged: running the untouched tree first")
        capture_relations(
            workdir,
            unchanged_plan(list(spec["checks"])),
            build_score_env(workdir, dict(os.environ) | dict(spec.get("env") or {})),
            baseline_dags=baseline_dags,
            replay_dags=spec.get("replay_dags"),
            out_dir=original_dir,
            arm="original",
        )
        lay_workspace()

    patch_text = spec["patch"].decode(errors="replace")
    base = {"config": spec["config"], "task_id": spec["task_id"], "trial": spec["trial"]}
    if patch_text.strip():
        with open("/tmp/changes.patch", "wb") as f:
            f.write(spec["patch"])
        subprocess.run(["git", "-C", workdir, "init", "-q"], check=False, capture_output=True)
        apply = subprocess.run(
            ["git", "-C", workdir, "apply", "--binary", "--whitespace=nowarn", "/tmp/changes.patch"],
            capture_output=True,
            text=True,
            check=False,
        )
        if apply.returncode != 0:
            progress(f"done in {time.monotonic() - started:.0f}s: patch did not apply")
            return {**base, "score_outcome": "apply_failed", "detail": (apply.stderr or "")[-400:]}

    # A live-env task is scored inside the future state the agent pinned: the
    # venv built from the PATCHED requirements.txt. Leaving the old pin means
    # being graded on the old Airflow — which is what "did not upgrade" means.
    # No requirements at all (astro projects) falls back to the image env; an
    # unbuildable pin set is itself the finding.
    env_note = None
    base_env = None
    env_source = "image (task declares no environment of its own)"
    if spec.get("live_env"):
        build = prepare_live_env(workdir, None)
        if build["env"] is not None:
            base_env = dict(os.environ) | build["env"]
            env_note = {"passed": True, "detail": build["detail"]}
            env_source = f"built from the patched requirements.txt: {build['detail']}"
        elif build["detail"] != "no airflow pin to build from":
            env_note = {"passed": False, "detail": build["detail"]}
            env_source = f"build failed: {build['detail']}"
        elif os.path.exists(os.path.join(workdir, "Dockerfile")):
            # An Astro Runtime project takes Airflow from its image and carries no
            # pin on purpose. Falling back is this project's real environment.
            env_source = "image (Astro Runtime project, no pin expected)"
        else:
            # A pip-managed repo that ends up declaring no Airflow gets graded on
            # ours. The trial still fails — the requirements check names the missing
            # pin — but its parse and run checks ran against an Airflow it never
            # asked for, and nothing said so. Recorded, not gated: making this fail
            # the environment would skip the very check that gives the diagnosis.
            env_source = "image fallback: no apache-airflow pin to build from"

    if spec.get("env"):
        # The world's own environment (dbt project and warehouse paths) is part
        # of the substrate every check runs on, same as the trial saw it.
        base_env = dict(base_env if base_env is not None else os.environ) | dict(spec["env"])

    progress(f"running {len(spec['checks'])} declared check(s)")
    progress(f"environment: {env_source}")
    result = score_workspace(
        workdir, list(spec["checks"]), list(spec["do_not_modify"]), patch_text,
        baseline_dags=baseline_dags, replay_dags=spec.get("replay_dags"),
        base_env=base_env, env_note=env_note, verifier_dir=verifier_dir,
        history=spec.get("history"), original_dir=original_dir,
        oracle_dir=oracle_dir,
    )
    progress(f"done in {time.monotonic() - started:.0f}s")
    return {**base, "score_outcome": "scored", "env_source": env_source, **result}
