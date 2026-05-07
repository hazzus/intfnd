"""End-to-end orchestration: ways → chains → detect → combine → score → dedupe → DB.

Each helper here is one stage of the pipeline. `run_pipeline` glues them together
and returns the process exit code. Stage boundaries match the ones documented in
the package docstring.
"""
import logging
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import rasterio
from tqdm import tqdm

from . import score as _score_mod
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
from .strip import strip_climbs

log = logging.getLogger(__name__)

# ── worker process state (set once per worker by _worker_init) ────────────────
_w_dem = None
_w_node_degree: dict | None = None
_w_progress_queue: multiprocessing.Queue | None = None


def _worker_init(
    dem_path: str,
    node_ways_map: dict,
    way_highways: dict,
    signal_coords: set,
    node_degree: dict,
    progress_queue: multiprocessing.Queue,
) -> None:
    global _w_dem, _w_node_degree, _w_progress_queue
    _w_dem = rasterio.open(dem_path)
    _w_node_degree = node_degree
    _w_progress_queue = progress_queue
    _score_mod.configure_intersection_lookups(node_ways_map, way_highways, signal_coords)


def _detect_chain_batch(job: tuple) -> tuple[list, dict, list | None]:
    """Worker entry point: process a batch of chains using process-local state."""
    chains, args, want_geojson = job
    detected: list[DetectedClimb] = []
    highway_counts: dict[str, int] = {}
    geojson_features: list[dict] | None = [] if want_geojson else None
    for chain in chains:
        _process_one_chain(chain, _w_dem, _w_node_degree, args, detected, highway_counts, geojson_features)
        _w_progress_queue.put(1)
    return detected, highway_counts, geojson_features


def _process_one_chain(
    chain: Chain,
    dem,
    node_degree: dict,
    args,
    detected_out: list,
    hc_out: dict,
    gjson_out: list | None,
) -> None:
    chain = rotate_loop_chain(chain, dem)
    if gjson_out is not None:
        gjson_out.append(chain_feature(chain))
    resampled = resample_way(chain.coords, args.sample_step)
    if resampled is None:
        return
    lats, lngs, cum = resampled
    if cum[-1] < args.min_length:
        return
    elev = sample_elevation(dem, lats, lngs)
    if np.any(np.isnan(elev)):
        return
    elev = smooth(elev, args.smooth_window, args.sample_step)

    chain_cum = cumulative_distances(chain.coords)
    chain_total = chain_cum[-1] if chain_cum else 0.0

    passes: list[tuple] = [(False, lats, lngs, cum, elev)]
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
                args.sample_step, node_degree, node_wids,
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
            detected_out.append(dc)
            hc_out[chain.highway] = hc_out.get(chain.highway, 0) + 1
            if gjson_out is not None:
                gjson_out.append(climb_feature(climb, chain))
            if args.verbose:
                log.info(
                    "climb: %-40s  %5.0f m  %4.1f%%  +%4.0f m  start=%.5f,%.5f",
                    chain_name[:40], climb.length_m, climb.grade * 100, climb.gain_m,
                    climb.coords[0][0], climb.coords[0][1],
                )


def detect_all(
    chains: list[Chain],
    dem_path: str,
    args,
    node_degree: dict[tuple[float, float], int],
    n_workers: int = 1,
    geojson_features: list[dict] | None = None,
) -> tuple[list[DetectedClimb], dict[str, int]]:
    """Run per-chain detection across every chain. Returns (detected, highway_counts)."""
    if n_workers > 1:
        return _detect_all_parallel(chains, dem_path, args, node_degree, n_workers, geojson_features)

    dem = rasterio.open(dem_path)
    try:
        detected: list[DetectedClimb] = []
        highway_counts: dict[str, int] = {}
        for chain in tqdm(chains, unit="chain"):
            _process_one_chain(chain, dem, node_degree, args, detected, highway_counts, geojson_features)
        return detected, highway_counts
    finally:
        dem.close()


def _detect_all_parallel(
    chains: list[Chain],
    dem_path: str,
    args,
    node_degree: dict,
    n_workers: int,
    geojson_features: list[dict] | None,
) -> tuple[list[DetectedClimb], dict[str, int]]:
    node_ways_map, way_highways, signal_coords = _score_mod.get_intersection_lookups()
    want_geojson = geojson_features is not None
    n_chains = len(chains)

    chunk_size = max(1, (n_chains + n_workers - 1) // n_workers)
    chunks = [chains[i : i + chunk_size] for i in range(0, n_chains, chunk_size)]

    progress_queue: multiprocessing.Queue = multiprocessing.Queue()
    jobs = [(chunk, args, want_geojson) for chunk in chunks]

    detected: list[DetectedClimb] = []
    highway_counts: dict[str, int] = {}

    with tqdm(total=n_chains, unit="chain") as bar:
        def _drain() -> None:
            seen = 0
            while seen < n_chains:
                progress_queue.get()
                bar.update(1)
                seen += 1

        drain = threading.Thread(target=_drain, daemon=True)
        drain.start()

        with ProcessPoolExecutor(
            max_workers=n_workers,
            initializer=_worker_init,
            initargs=(dem_path, node_ways_map, way_highways, signal_coords, node_degree, progress_queue),
        ) as executor:
            futures = [executor.submit(_detect_chain_batch, job) for job in jobs]
            for future in as_completed(futures):
                batch_detected, batch_hc, batch_gjson = future.result()
                detected.extend(batch_detected)
                for hw, n in batch_hc.items():
                    highway_counts[hw] = highway_counts.get(hw, 0) + n
                if geojson_features is not None and batch_gjson:
                    geojson_features.extend(batch_gjson)

        drain.join()

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
    n_workers = getattr(args, "workers", 1)

    detected, highway_counts = detect_all(
        chains, dem.name, args, node_degree,
        n_workers=n_workers,
        geojson_features=geojson_features,
    )

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

    before_strip = len(detected)
    detected, stripped, strip_dropped = strip_climbs(
        detected,
        args.max_strip,
        args.strip_degree,
        args.sample_step,
        args.min_length,
        args.min_grade,
        args.min_gain,
        node_degree,
        debug=getattr(args, "debug_strip", False),
    )
    log.info(
        "stripped %d / %d climbs at sharp intersections within %.0f m (>= %.0f°); dropped %d sub-threshold",
        stripped, before_strip, args.max_strip, args.strip_degree, strip_dropped,
    )

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
