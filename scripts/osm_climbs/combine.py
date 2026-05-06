"""Build multi-chain "combination" climbs that span junctions.

The chain stitcher refuses to merge across highway-class transitions; combinations
are how a tertiary-then-primary ramp still gets emitted as one climb. We DFS the
climb-adjacency graph (climbs share a junction node), build the joined polyline,
and re-run the detector on it. A combination at depth k must straddle every
junction on the path — otherwise it's just rediscovering a shorter combination.
"""
from collections import defaultdict

import numpy as np

from .detect import (
    Climb,
    DetectedClimb,
    detect_climbs,
    unique_in_order,
)
from .elevation import sample_elevation, smooth
from .geo import cumulative_distances, resample_way
from .geojson_out import combination_feature
from .score import score_breakdown


def combo_name(names: list[str]) -> str:
    return " + ".join(unique_in_order(names))


def build_combinations(
    detected: list[DetectedClimb],
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
    geojson_features: list[dict] | None = None,
) -> list[DetectedClimb]:
    """For each ordered sequence of up to --max-combo climbs sharing OSM nodes at consecutive
    junctions, build C0_start → N1 → ... → Nk-1 → Ck_end and re-run the climb detector on the
    joined polyline.

    Combinations are explicitly allowed to cross highway types — that's the whole point,
    since the chain stitcher refuses to merge across highway-class transitions.

    Walks the climb-adjacency graph by DFS up to depth max_combo. At depth k there are k-1
    junctions; we require the detected climb to straddle all of them, so a k-deep combo that
    only spans some junctions is dropped (it was already emitted at a shallower depth).
    """
    if not detected:
        return []
    max_len = max(2, int(args.max_combo))

    # Index nodes → (climb_idx, position_in_climb).
    node_map: dict[tuple[float, float], list[tuple[int, int]]] = defaultdict(list)
    for ci, dc in enumerate(detected):
        for pos, node in enumerate(dc.nodes):
            node_map[node].append((ci, pos))

    processed_paths: set[tuple[int, ...]] = set()
    out: list[DetectedClimb] = []

    def emit(path: list[int], splits: list[tuple[int, int]]) -> None:
        first = detected[path[0]]
        first_p = splits[0][0]
        combined_nodes: list[tuple[float, float]] = list(first.nodes[: first_p + 1])
        combined_node_way_ids: list[int] = list(first.node_way_ids[: first_p + 1])
        combined_node_surfaces: list[str] = list(first.node_surfaces[: first_p + 1])
        junction_indices = [first_p]
        for i in range(1, len(path)):
            mid = detected[path[i]]
            q_prev = splits[i - 1][1]
            seg_end = splits[i][0] + 1 if i < len(path) - 1 else len(mid.nodes)
            combined_nodes.extend(mid.nodes[q_prev + 1 : seg_end])
            combined_node_way_ids.extend(mid.node_way_ids[q_prev + 1 : seg_end])
            combined_node_surfaces.extend(mid.node_surfaces[q_prev + 1 : seg_end])
            if i < len(path) - 1:
                junction_indices.append(len(combined_nodes) - 1)

        if len(combined_nodes) < 2:
            return
        combined_node_cum = cumulative_distances(combined_nodes)
        if combined_node_cum[-1] < args.min_length:
            return
        junction_dists = [combined_node_cum[idx] for idx in junction_indices]

        resampled = resample_way(combined_nodes, args.sample_step)
        if resampled is None:
            return
        lats, lngs, cum = resampled
        elev = sample_elevation(dem, lats, lngs)
        if np.any(np.isnan(elev)):
            return
        elev = smooth(elev, args.smooth_window, args.sample_step)

        members = [detected[ci] for ci in path]
        unique_highways = unique_in_order([m.highway for m in members])
        highway = unique_highways[0] if len(unique_highways) == 1 else "+".join(unique_highways)
        name = combo_name([m.name for m in members])

        for climb, ti, pi in detect_climbs(
            lats, lngs, cum, elev,
            args.min_length, args.min_grade, args.min_gain, args.prominence,
        ):
            start_d = float(cum[ti])
            end_d = float(cum[pi])
            # The detected climb must straddle every junction on the path; otherwise the
            # detector just rediscovered a shorter combination already emitted at a
            # shallower DFS depth (or a single climb in isolation).
            if not all(start_d <= jd <= end_d for jd in junction_dists):
                continue
            climb_nodes: list[tuple[float, float]] = []
            climb_wids: list[int] = []
            climb_surfs: list[str] = []
            for cn, cw, cs, cd in zip(
                combined_nodes, combined_node_way_ids, combined_node_surfaces, combined_node_cum
            ):
                if start_d <= cd <= end_d:
                    climb_nodes.append(cn)
                    climb_wids.append(cw)
                    climb_surfs.append(cs)
            elevation_profile = [float(x) for x in elev[ti : pi + 1]]
            sb = score_breakdown(
                climb_nodes, elevation_profile, climb.length_m,
                args.sample_step, node_degree, climb_wids,
            )
            dc = DetectedClimb(
                climb=climb,
                name=name,
                surfaces=unique_in_order(climb_surfs),
                highway=highway,
                osm_way_ids=unique_in_order(climb_wids),
                bidirectional=False,
                elevation_profile=elevation_profile,
                nodes=climb_nodes,
                node_way_ids=climb_wids,
                node_surfaces=climb_surfs,
                is_combination=True,
                score=sb["score"],
                score_components=sb,
            )
            out.append(dc)
            if geojson_features is not None:
                geojson_features.append(combination_feature(dc))

    def extend(path: list[int], splits: list[tuple[int, int]], current_q: int) -> None:
        if len(path) >= max_len:
            return
        curr = detected[path[-1]]
        # Exit positions strictly after where we entered the current climb, so each
        # member contributes at least one node to the combined polyline.
        for p in range(current_q + 1, len(curr.nodes)):
            for cj, qj in node_map[curr.nodes[p]]:
                if cj in path:
                    continue
                # Need real tail past the junction in the next climb.
                if qj >= len(detected[cj].nodes) - 1:
                    continue
                new_path = path + [cj]
                key = tuple(new_path)
                if key in processed_paths:
                    continue
                processed_paths.add(key)
                new_splits = splits + [(p, qj)]
                emit(new_path, new_splits)
                extend(new_path, new_splits, qj)

    for ci in range(len(detected)):
        extend([ci], [], 0)

    return out
