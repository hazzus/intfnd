#!/usr/bin/env python3

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import psycopg2

from chain import build_chains, chain_info
from climb import process_climbs
from combine import combine_climbs
from debug import debug_way
from dedupe import dedupe_climbs
from degree import fill_node_degrees, degree_distribution
from elevation import fill_elevations
from osm_load import load_data
from score import score_climbs
from strip import strip_climbs

log = logging.getLogger(__name__)

STEPS = ["load", "degree", "elevation", "stitch", "climbs", "strip", "combine", "dedupe", "score"]

_REGION_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_SCHEMA_SQL = Path(__file__).with_name("schema.sql")


def _ensure_region_schema(conn, region: str, reset: bool):
    """Create the region's schema and intermediate tables (idempotent).

    search_path already points at <region>,public (via PGOPTIONS), so the DDL in
    schema.sql lands in the region schema. With reset=True the schema is dropped first
    for a clean rebuild.
    """
    with conn.cursor() as cur:
        if reset:
            log.info("resetting schema %s", region)
            cur.execute(f'DROP SCHEMA IF EXISTS "{region}" CASCADE')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{region}"')
        cur.execute(_SCHEMA_SQL.read_text())
    conn.commit()


def parse_args():
    parser = argparse.ArgumentParser(description="OSM climb pipeline")
    parser.add_argument("--pbf", type=Path, help="OSM PBF file (required for step: load)")
    parser.add_argument("--dem", type=Path, help="DEM raster file (required for step: elevation)")
    parser.add_argument("--db", required=True, help="PostgreSQL connection string")
    parser.add_argument("--region", required=True,
                        help="Region name; isolates intermediate tables in a schema of this name "
                             "and tags climbs. Must match [a-z_][a-z0-9_]*")
    parser.add_argument("--reset-region", action="store_true",
                        help="Drop the region's schema before running (clean rebuild)")
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS, metavar="STEP",
                        help=f"pipeline steps to run (default: all); choices: {', '.join(STEPS)}")
    parser.add_argument("--sample-step", type=float, default=10.0, help="Resample spacing (m)")
    parser.add_argument("--smooth-window", type=float, default=100.0, help="Elevation smoothing window (m)")
    parser.add_argument("--min-length", type=float, default=100.0, help="Min climb length (m)")
    parser.add_argument("--min-grade", type=float, default=0.01, help="Min average grade (decimal)")
    parser.add_argument("--min-gain", type=float, default=0.0, help="Min elevation gain (m)")
    parser.add_argument("--prominence", type=float, default=15.0, help="Peak prominence threshold (m)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="logging level (default: INFO)")
    parser.add_argument("--workers", type=int, default=1, help="Workers amount in case of parallel work")
    parser.add_argument("--max-strip", type=float, default=50.0, help="Max meters to strip from start/end in strip step")
    parser.add_argument("--strip-degree", type=float, default=45.0, help="Minimum turn angle (degrees) to trigger stripping")
    parser.add_argument("--max-combo", type=int, default=4, help="Max proto_climbs to chain in combine step (>= 2)")
    parser.add_argument("--max-similarity", type=float, default=0.9, help="Jaccard similarity threshold for dedupe step (0–1)")
    parser.add_argument("--debug-way", type=int, metavar="WAY_ID",
                        help="Show step-by-step debug output for the chain containing a way and exit")
    return parser.parse_args()


_STEP_REQUIRED_ARGS: dict[str, list[str]] = {
    "load": ["pbf"],
    "elevation": ["dem"],
}


def check_args(args) -> list[str]:
    errors = []
    for step in args.steps:
        for arg in _STEP_REQUIRED_ARGS.get(step, []):
            val: Path | None = getattr(args, arg)
            if not val:
                errors.append(f"--{arg} is required for step '{step}'")
            elif not val.exists():
                errors.append(f"--{arg} file not found: {val}")
    return errors


def _climb_params(args) -> dict:
    return dict(
        sample_step=args.sample_step,
        smooth_window=args.smooth_window,
        min_length=args.min_length,
        min_grade=args.min_grade,
        min_gain=args.min_gain,
        prominence=args.prominence,
    )


def main():
    args = parse_args()
    debug_run = args.debug_way is not None
    logging.basicConfig(
        level="DEBUG" if debug_run else args.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not _REGION_RE.match(args.region):
        print(f"error: invalid --region {args.region!r} (must match [a-z_][a-z0-9_]*)", file=sys.stderr)
        sys.exit(1)

    if errors := check_args(args):
        for e in errors:
            print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    # Route all intermediate-table access (every psycopg2.connect, including worker
    # processes that inherit os.environ) to the region's schema. climbs has no copy in
    # the region schema, so it falls through to public — shared across regions.
    os.environ["PGOPTIONS"] = f"-c search_path={args.region},public"

    if debug_run:
        debug_way(args.db, args.debug_way, **_climb_params(args),
                  max_strip=args.max_strip, strip_degree=args.strip_degree)
        return

    steps = args.steps
    conn = psycopg2.connect(args.db)
    try:
        _ensure_region_schema(conn, args.region, args.reset_region)

        if "load" in steps:
            log.info("step: load")
            load_data(str(args.pbf), conn)

        if "degree" in steps:
            log.info("step: degree")
            fill_node_degrees(conn)
            log.info("node degrees done")
            for degree, count in degree_distribution(conn).items():
                log.debug("  degree %d: %d nodes", degree, count)

        if "elevation" in steps:
            log.info("step: elevation")
            fill_elevations(conn, args.dem)

        if "stitch" in steps:
            log.info("step: stitching chains")
            build_chains(conn)
            log.debug(chain_info(conn))

        if "climbs" in steps:
            log.info("step: climbs")
            process_climbs(args.db, **_climb_params(args), workers=args.workers)

        if "combine" in steps:
            log.info("step: combine")
            n = combine_climbs(args.db, max_combo=args.max_combo)
            log.info("combine done: %d climbs inserted", n)
        
        if "strip" in steps:
            log.info("step: strip")
            n = strip_climbs(args.db, max_strip_m=args.max_strip, strip_degree=args.strip_degree)
            log.info("strip done: %d proto_climbs modified", n)

        if "dedupe" in steps:
            log.info("step: dedupe")
            n = dedupe_climbs(args.db, max_similarity=args.max_similarity)
            log.info("dedupe done: %d proto_climbs deleted", n)

        if "score" in steps:
            log.info("step: score")
            n = score_climbs(args.db, args.region)
            log.info("score done: %d climbs inserted", n)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
