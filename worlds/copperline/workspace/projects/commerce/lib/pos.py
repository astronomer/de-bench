"""Reading the store batch tree.

`store_pos_intake` and `store_pos_late_catchup` both walk
`landing/pos/dt=<ds>/`, so the walk lives here rather than in one DAG that the
other imports. `docs/runbooks/pos-ingestion.md` describes what arrives.

**The store code is in the file name, not in the file.** The store server writes
its own columns and its own name, and the name is the only place the store
appears. That is why every file is loaded on its own rather than through one
glob, and it is why a file renamed on the way in lands under the wrong store.
"""

from __future__ import annotations

from include.lib import landing_dir

__all__ = ["FILE_PREFIX", "store_files", "store_file"]

#: `store_S-0117.csv` — the store code sits between the prefix and the suffix.
FILE_PREFIX = "store_"


def store_files(ds: str) -> list[tuple[str, str]]:
    """(store code, path) for every batch file on disk for a business date.

    Sorted by store, so two runs over the same day load in the same order. An
    empty list means no file has landed yet, which POS-1 calls late rather than
    missing.
    """
    directory = landing_dir("pos") / f"dt={ds}"
    if not directory.is_dir():
        return []
    return sorted(
        (path.stem[len(FILE_PREFIX):], str(path))
        for path in directory.glob(f"{FILE_PREFIX}*.csv")
    )


def store_file(ds: str, store: str) -> str | None:
    """One store's file for a business date, or None if it has not landed."""
    return dict(store_files(ds)).get(store)
