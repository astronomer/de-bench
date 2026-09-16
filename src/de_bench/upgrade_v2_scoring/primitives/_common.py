"""Shared helpers for scoring primitives.

`read_text_capped` is the read-with-safety-checks primitives use
instead of `path.read_text`. Primitives walk arbitrary files in the
working directory — an agent that plants a 500 MB .py file or a
binary would otherwise exhaust memory on `read_text` and trigger
pathological regex / AST behaviour.
"""

from __future__ import annotations

from pathlib import Path

# 1 MB. Real DAG files are ~1-50 KB; a file above this is either a
# bug or an attack. Skipping the file and treating it as "not clean"
# is the right response for a deprecated-check primitive.
MAX_FILE_BYTES = 1_000_000


def read_text_capped(path: Path) -> str | None:
    """Read `path` as utf-8 text, or return `None` if it can't be
    safely read (oversized, unreadable, invalid encoding)."""
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if size > MAX_FILE_BYTES:
        return None
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
