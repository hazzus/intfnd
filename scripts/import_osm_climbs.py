#!/usr/bin/env python3
"""Extract climbs from OSM PBF + DEM and insert into the climbs table.

Not idempotent: re-running on the same input will create duplicate rows.

Example:
    python import_osm_climbs.py \\
        --pbf liechtenstein.osm.pbf \\
        --dem liechtenstein-dem.tif \\
        --db postgres://postgres:pw@localhost/intfnd

The actual pipeline lives in the `osm_climbs` package next to this entry script;
each module there corresponds to one stage of the flow.
"""
import argparse
import logging
import sys

import rasterio

from osm_climbs import score
from osm_climbs.chains import build_chains, compute_node_degree, compute_node_ways
from osm_climbs.debug import run_debug
from osm_climbs.osm_load import load_ways
from osm_climbs.pipeline import run_pipeline

log = logging.getLogger("osm_climbs")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--pbf", required=True, help="Path to .osm.pbf input")
    ap.add_argument("--dem", required=True, help="Path to DEM GeoTIFF (EPSG:4326)")
    ap.add_argument("--db", required=True, help="Postgres DSN")
    ap.add_argument("--min-length", type=float, default=300.0, help="Min climb length for output (m)")
    ap.add_argument("--min-grade", type=float, default=0.01,
                    help="Min average grade for output (decimal). Candidates with grade <= 0 are always rejected.")
    ap.add_argument("--min-gain", type=float, default=0.0, help="Min elevation gain (m); 0 disables")
    ap.add_argument("--sample-step", type=float, default=10.0, help="Resample spacing (m)")
    ap.add_argument("--smooth-window", type=float, default=100.0, help="Elevation smoothing window (m)")
    ap.add_argument("--prominence", type=float, default=10.0, help="Peak prominence threshold (m)")
    ap.add_argument("--max-combo", type=int, default=4, help="Max climbs to chain into a combination (>= 2)")
    ap.add_argument("--max-strip", type=float, default=200.0,
                    help="Max distance (m) from a climb's start to look for a sharp intersection. "
                         "If hit, the prefix is stripped and the climb restarts at the intersection.")
    ap.add_argument("--strip-degree", type=float, default=45.0,
                    help="Min turn angle (degrees) at an intersection to trigger prefix stripping.")
    ap.add_argument("--debug-strip", action="store_true",
                    help="Print a per-climb trace of the strip stage's decisions: nodes walked "
                         "within --max-strip, intersection/turn checks, and the final outcome "
                         "(stripped, dropped sub-threshold, or kept unchanged).")
    ap.add_argument("--max-similarity", type=float, default=0.85,
                    help="Drop climbs whose node-set Jaccard overlap with another's >= this; "
                         "the climb with the lower score survives. Use 1.0 to disable.")
    ap.add_argument("--dry-run", action="store_true", help="Print stats, don't insert")
    ap.add_argument("-v", "--verbose", action="store_true", help="Print each detected climb")
    ap.add_argument(
        "--out-geojson",
        help="Write candidate ways + detected climbs to a GeoJSON file for visual inspection",
    )
    ap.add_argument(
        "--debug-way",
        type=int,
        action="append",
        metavar="OSM_WAY_ID",
        help="Find the chain containing this OSM way id and dump its full pipeline trace. Repeatable.",
    )
    ap.add_argument(
        "--debug-plot",
        metavar="PREFIX",
        help="Save elevation profile PNG when --debug-way is set. "
             "Pass a prefix like 'debug/' (saved to debug/way_<id>.png) or 'profile' (profile_<id>.png).",
    )
    ap.add_argument("--log-level", default="INFO")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    log.info("loading ways from %s", args.pbf)
    ways, signal_coords = load_ways(args.pbf)
    if not ways:
        log.error("no cycling network found in %s", args.pbf)
        return 1
    log.info("loaded %d ways, %d traffic-signal nodes", len(ways), len(signal_coords))

    log.info("computing node→ways map")
    node_ways_map = compute_node_ways(ways)
    node_degree = compute_node_degree(ways)
    way_highways = {w.id: w.highway for w in ways}
    score.configure_intersection_lookups(node_ways_map, way_highways, signal_coords)

    log.info("stitching chains")
    chains = build_chains(ways)
    log.info(
        "built %d chains (avg %.1f ways/chain, max %d)",
        len(chains),
        sum(len(c.way_ids) for c in chains) / max(1, len(chains)),
        max((len(c.way_ids) for c in chains), default=0),
    )

    log.info("opening DEM %s", args.dem)
    dem = rasterio.open(args.dem)
    if dem.crs and dem.crs.to_epsg() != 4326:
        log.warning("DEM CRS is %s, expected EPSG:4326 — sampling may be incorrect", dem.crs)

    try:
        if args.debug_way:
            run_debug(chains, ways, set(args.debug_way), dem, args, node_degree)
            return 0
        return run_pipeline(chains, ways, dem, args, node_degree)
    finally:
        dem.close()


if __name__ == "__main__":
    sys.exit(main())
