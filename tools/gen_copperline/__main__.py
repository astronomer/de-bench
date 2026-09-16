import argparse

from . import build


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="gen_copperline",
        description="Generate the copperline warehouse and landing tree.",
    )
    p.add_argument("--timeline", required=True, help="timeline.yaml")
    p.add_argument("--planted", required=True, help="planted.yaml")
    p.add_argument("--db", required=True, help="DuckDB file to write (replaced)")
    p.add_argument("--landing", required=True, help="directory for the dated landing files")
    p.add_argument("--profile", choices=("full", "small"), default="full",
                   help="small scales fact volumes down ~20x, keeps entities and planted rows")
    p.add_argument("--render-planted", metavar="PATH", default=None,
                   help="also render PLANTED.md's generated half to PATH (repo-side use)")
    a = p.parse_args(argv)
    build.run(
        timeline_path=a.timeline,
        planted_path=a.planted,
        db_path=a.db,
        landing_dir=a.landing,
        profile=a.profile,
        render_planted=a.render_planted,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
