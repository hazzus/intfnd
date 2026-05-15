#!/usr/bin/env python3

import argparse
import logging
import sys
from pathlib import Path

import psycopg2

from chain import build_chains, chain_info
from climb import process_climbs, debug_chain
from degree import fill_node_degrees, degree_distribution
from elevation import fill_elevations
from osm_load import load_data

log = logging.getLogger(__name__)

STEPS = ["load", "degree", "elevation", "stitch", "climbs"]


def parse_args():
    parser = argparse.ArgumentParser(description="OSM climb pipeline")
    parser.add_argument("--pbf", type=Path, help="OSM PBF file (required for step: load)")
    parser.add_argument("--dem", type=Path, help="DEM raster file (required for step: elevation)")
    parser.add_argument("--db", required=True, help="PostgreSQL connection string")
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS, metavar="STEP",
                        help=f"pipeline steps to run (default: all); choices: {', '.join(STEPS)}")
    parser.add_argument("--sample-step", type=float, default=10.0, help="Resample spacing (m)")
    parser.add_argument("--smooth-window", type=float, default=100.0, help="Elevation smoothing window (m)")
    parser.add_argument("--min-length", type=float, default=300.0, help="Min climb length (m)")
    parser.add_argument("--min-grade", type=float, default=0.01, help="Min average grade (decimal)")
    parser.add_argument("--min-gain", type=float, default=10.0, help="Min elevation gain (m)")
    parser.add_argument("--prominence", type=float, default=15.0, help="Peak prominence threshold (m)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="logging level (default: INFO)")
    parser.add_argument("--workers", type=int, default=1, help="Workers amount in case of parallel work")
    parser.add_argument("--debug-chain", type=int, metavar="CHAIN_ID",
                        help="Show step-by-step debug output for a single chain and exit")
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
    debug_run = args.debug_chain is not None
    logging.basicConfig(
        level="DEBUG" if debug_run else args.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if errors := check_args(args):
        for e in errors:
            print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    if debug_run:
        debug_chain(args.db, args.debug_chain, **_climb_params(args))
        return

    steps = args.steps
    conn = psycopg2.connect(args.db)
    try:
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

    finally:
        conn.close()


if __name__ == "__main__":
    main()
