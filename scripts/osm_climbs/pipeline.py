"""End-to-end orchestration: ways → chains → detect → combine → score → dedupe → DB.

Each helper here is one stage of the pipeline. `run_pipeline` glues them together
and returns the process exit code. Stage boundaries match the ones documented in
the package docstring.
"""
import logging

import numpy as np
from tqdm import tqdm

from .chains import (
    Chain,
    chain_display_name,
    rotate_loop_chain,
)
from .combine import build_combinations
from .db import insert_climbs, to_climb_row
from .dedupe import deduplicate_climbs
from .detect import (
    DetectedClimb,
    chain_node_slice,
    detect_climbs,
    unique_in_order,
)
from .elevation import reverse_profile, sample_elevation, smooth
from .geo import cumulative_distances, resample_way
from .geojson_out import chain_feature, climb_feature, write_geojson
from .osm_load import Way
from .score import log_score_stats, score_breakdown

log = logging.getLogger(__name__)


def detect_all(
    chains: list[Chain],
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
    geojson_features: list[dict] | None = None,
) -> tuple[list[DetectedClimb], dict[str, int]]:
    """Run per-chain detection across every chain. Returns (detected, highway_counts)."""
    detected: list[DetectedClimb] = []
    highway_counts: dict[str, int] = {}

    for chain in tqdm(chains, unit="chain"):
        chain = rotate_loop_chain(chain, dem)
        if geojson_features is not None:
            geojson_features.append(chain_feature(chain))
        resampled = resample_way(chain.coords, args.sample_step)
        if resampled is None:
            continue
        lats, lngs, cum = resampled
        if cum[-1] < args.min_length:
            continue
        elev = sample_elevation(dem, lats, lngs)
        if np.any(np.isnan(elev)):
            continue
        elev = smooth(elev, args.smooth_window, args.sample_step)

        chain_cum = cumulative_distances(chain.coords)
        chain_total = chain_cum[-1] if chain_cum else 0.0

        passes: list[tuple[bool, np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = [
            (False, lats, lngs, cum, elev),
        ]
        if chain.bidirectional:
            rl, rln, rc, re_ = reverse_profile(lats, lngs, cum, elev)
            passes.append((True, rl, rln, rc, re_))

        chain_name = chain_display_name(chain)

        for reversed_pass, p_lats, p_lngs, p_cum, p_elev in passes:
            for climb, ti, pi in detect_climbs(
                p_lats, p_lngs, p_cum, p_elev,
                args.min_length, args.min_grade, args.min_gain, args.prominence,
            ):
                nodes, node_wids, node_surfs = chain_node_slice(
                    chain, chain_cum, chain_total,
                    p_cum, ti, pi, reversed_pass,
                )
                elevation_profile = [float(x) for x in p_elev[ti : pi + 1]]
                sb = score_breakdown(
                    nodes, elevation_profile, climb.length_m,
                    args.sample_step, node_degree,
                )
                dc = DetectedClimb(
                    climb=climb,
                    name=chain_name,
                    surfaces=unique_in_order(node_surfs),
                    highway=chain.highway,
                    osm_way_ids=unique_in_order(node_wids),
                    bidirectional=chain.bidirectional,
                    elevation_profile=elevation_profile,
                    nodes=nodes,
                    node_way_ids=node_wids,
                    node_surfaces=node_surfs,
                    score=sb["score"],
                    score_components=sb,
                )
                detected.append(dc)
                highway_counts[chain.highway] = highway_counts.get(chain.highway, 0) + 1
                if geojson_features is not None:
                    geojson_features.append(climb_feature(climb, chain))
                if args.verbose:
                    log.info(
                        "climb: %-40s  %5.0f m  %4.1f%%  +%4.0f m  start=%.5f,%.5f",
                        chain_name[:40],
                        climb.length_m,
                        climb.grade * 100,
                        climb.gain_m,
                        climb.coords[0][0],
                        climb.coords[0][1],
                    )

    return detected, highway_counts


def log_size_stats(detected: list[DetectedClimb], highway_counts: dict[str, int]) -> None:
    if highway_counts:
        breakdown = ", ".join(
            f"{hw}={n}" for hw, n in sorted(highway_counts.items(), key=lambda kv: -kv[1])
        )
        log.info("by highway: %s", breakdown)
    lengths = [dc.climb.length_m for dc in detected]
    grades = [dc.climb.grade for dc in detected]
    if lengths:
        ls = np.array(lengths)
        gs = np.array(grades) * 100
        log.info(
            "length (m): min=%.0f median=%.0f max=%.0f",
            ls.min(), np.median(ls), ls.max(),
        )
        log.info(
            "grade  (%%): min=%.1f median=%.1f max=%.1f",
            gs.min(), np.median(gs), gs.max(),
        )


def run_pipeline(
    chains: list[Chain],
    ways: list[Way],
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
) -> int:
    """Full detection + insertion run. Returns the process exit code."""
    geojson_features: list[dict] | None = [] if args.out_geojson else None

    detected, highway_counts = detect_all(chains, dem, args, node_degree, geojson_features)

    log.info("detected %d per-chain climbs; building combinations", len(detected))
    combinations = build_combinations(
        detected, dem, args, node_degree,
        geojson_features=geojson_features,
    )
    log.info("built %d combination climbs", len(combinations))
    for dc in combinations:
        highway_counts[dc.highway] = highway_counts.get(dc.highway, 0) + 1
        if args.verbose:
            log.info(
                "combo: %-40s  %5.0f m  %4.1f%%  +%4.0f m",
                dc.name[:40],
                dc.climb.length_m,
                dc.climb.grade * 100,
                dc.climb.gain_m,
            )
    detected.extend(combinations)

    before = len(detected)
    detected, dropped = deduplicate_climbs(detected, args.max_similarity)
    log.info(
        "deduplicated: %d → %d climbs (dropped %d at similarity >= %.2f)",
        before, len(detected), dropped, args.max_similarity,
    )

    rows = [to_climb_row(dc) for dc in detected]

    log.info("detected %d climbs total", len(rows))
    log_size_stats(detected, highway_counts)
    log_score_stats(detected)

    if geojson_features is not None:
        write_geojson(args.out_geojson, geojson_features)
        log.info("wrote %d features to %s", len(geojson_features), args.out_geojson)

    if args.dry_run:
        log.info("--dry-run: not inserting")
        return 0

    log.info("inserting %d climbs", len(rows))
    insert_climbs(rows, args.db)
    log.info("done")
    return 0
