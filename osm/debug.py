import logging

import numpy as np
import psycopg2
from scipy.ndimage import uniform_filter1d

from chain import get_chain
from climb import (
    _build_chain,
    _chain_node_slice,
    _cumulative_distances,
    _find_extrema,
    _nodes_hash,
    _resample,
    _rotate_cycle,
    _unique_in_order,
)
from geo import node_dist
from score import score_proto
from strip import _strip_ends

log = logging.getLogger(__name__)


def _show_proto_climb(
    conn: psycopg2.extensions.connection,
    nids: list[int],
    max_strip: float,
    strip_degree: float,
) -> None:
    p = print
    h = _nodes_hash(nids)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, nodes, osm_way_ids, start_lat, start_lng, distance, from_climbs
            FROM proto_climbs WHERE nodes_hash = %s
            """,
            (h,),
        )
        row = cur.fetchone()
    if row is None:
        p(f"      proto_climb: NOT FOUND (nodes_hash={h})")
        return
    pid, nodes, way_ids, start_lat, start_lng, distance, from_climbs = row
    nodes = list(nodes)
    p(f"      proto_climb id: {pid}")
    p(f"        nodes_hash:   {h}")
    p(f"        osm_way_ids:  {way_ids}")
    p(f"        start:        ({start_lat:.6f}, {start_lng:.6f})")
    p(f"        distance:     {distance:.1f}m")
    p(f"        nodes:        {len(nodes)}")
    p(f"        from_climbs:  {from_climbs}")

    # strip stage — runs before scoring in the real pipeline
    with conn.cursor() as cur:
        cur.execute("SELECT id, lat, lng FROM nodes WHERE id = ANY(%s)", (nodes,))
        latlon_map = {r[0]: (float(r[1]), float(r[2])) for r in cur.fetchall()}
    if any(nid not in latlon_map for nid in nodes):
        p("        strip/score: SKIPPED (missing nodes)")
        return
    latlon = [latlon_map[nid] for nid in nodes]

    orig_n = len(nodes)
    s, e = _strip_ends(latlon, max_strip, strip_degree)
    if s == 0 and e == orig_n:
        p(f"        strip: no change (max_strip={max_strip:.0f}m, strip_degree={strip_degree:.0f}°)")
    else:
        nodes = nodes[s:e]
        latlon = latlon[s:e]
        distance = sum(node_dist(*latlon[i - 1], *latlon[i]) for i in range(1, len(latlon)))
        start_lat, start_lng = latlon[0]
        p(f"        strip: -{s} from start, -{orig_n - e} from end  "
          f"({orig_n} → {len(nodes)} nodes)")
        p(f"          new start:    ({start_lat:.6f}, {start_lng:.6f})")
        p(f"          new distance: {distance:.1f}m")

    # run the real scorer on the (possibly stripped) proto_climb
    scored = score_proto(conn, nodes, way_ids, start_lat, start_lng, distance)
    if scored is None:
        p("        score: SKIPPED (missing nodes)")
        return
    _, b = scored
    p(f"        name:          {b['name']!r}")
    p(f"        average_grade: {b['average_grade']:.2f}%")
    p(f"        surfaces:      {b['surfaces']}  is_paved={b['is_paved']}  bidirectional={b['bidirectional']}")
    p(f"        elevation_profile: {b['n_profile']} pts   polyline: {b['polyline_len']} chars")
    p(f"        SCORE: {b['score']:.1f}")
    p(f"          signals:       {b['signal_penalty']:8.1f}  ({b['n_signals']} signals)")
    p(f"          intersections: {b['intersection_penalty']:8.1f}  ({b['n_intersections']} crossings)")
    p(f"          turns:         {b['turn_penalty']:8.1f}  ({b['n_sharp_turns']} sharp turns)")
    for lat, lng, angle, pen in b["turns"]:
        p(f"            +{pen:6.1f}  {angle:5.1f}°  ({lat:.6f}, {lng:.6f})")
    p(f"          spikes:        {b['spike_penalty']:8.1f}")


def debug_way(
    dsn: str,
    way_id: int,
    sample_step: float = 10.0,
    smooth_window: float = 100.0,
    min_length: float = 300.0,
    min_grade: float = 0.01,
    min_gain: float = 0.0,
    prominence: float = 10.0,
    max_strip: float = 50.0,
    strip_degree: float = 45.0,
) -> None:
    p = print
    p(f"\n=== way {way_id} ===")

    conn = psycopg2.connect(dsn)
    try:
        # 1. find the chain this way belongs to
        with conn.cursor() as cur:
            cur.execute("SELECT chain_id FROM ways WHERE id = %s", (way_id,))
            row = cur.fetchone()
        if row is None:
            p(f"FAIL: way {way_id} not found in ways table")
            return
        chain_id = row[0]
        if chain_id is None:
            p(f"FAIL: way {way_id} has no chain_id (run the 'stitch' step first)")
            return
        p(f"chain_id: {chain_id}")

        # 2. all ways in this chain
        way_ids = get_chain(conn, chain_id)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, highway, surface, bidirectional, array_length(nodes, 1) "
                "FROM ways WHERE id = ANY(%s)",
                (way_ids,),
            )
            way_details = {r[0]: r for r in cur.fetchall()}
        p(f"ways in chain ({len(way_ids)}):")
        for wid in way_ids:
            mark = " <-- target" if wid == way_id else ""
            if wid in way_details:
                _, highway, surface, bidir, n_nodes = way_details[wid]
                p(f"  way {wid}: highway={highway!r} surface={surface!r} "
                  f"bidirectional={bidir} nodes={n_nodes}{mark}")
            else:
                p(f"  way {wid}: MISSING from ways table{mark}")

        # 3. sorted/stitched into a single connected way
        chain = _build_chain(conn, chain_id)
        if isinstance(chain, str):
            p(f"FAIL: {chain}")
            return

        coords = chain["coords"]
        elevations = chain["elevations"]
        ordered_ways = _unique_in_order(chain["way_per_coord"])
        p(f"\nstitched order ({len(ordered_ways)} ways): {ordered_ways}")
        p(f"coords: {len(coords)}  is_cycle={chain['is_cycle']}  "
          f"bidirectional={chain['bidirectional']}  highway={chain['highway']!r}")
        p(f"elevation range: {min(elevations):.1f}m – {max(elevations):.1f}m")

        # 4. rotate cycles so the lowest point is first
        if chain["is_cycle"]:
            chain = _rotate_cycle(chain)
            coords = chain["coords"]
            elevations = chain["elevations"]
            p(f"rotated cycle: elevation now {elevations[0]:.1f}m – {elevations[-1]:.1f}m")

        # 5. run climb detection on the stitched chain
        resampled = _resample(coords, sample_step)
        if resampled is None:
            p("FAIL: _resample returned None")
            return
        lats, lngs, cum = resampled
        chain_total = cum[-1]
        p(f"\nresampled: {len(lats)} points  total={chain_total:.1f}m  step={sample_step}m")

        if chain_total < min_length:
            p(f"SKIP: {chain_total:.1f}m < min_length {min_length:.1f}m")
            return

        node_cum = np.array(_cumulative_distances(coords))
        elev = np.interp(cum, node_cum, np.array(elevations, dtype=float))

        w = max(1, int(round(smooth_window / sample_step)))
        if 1 < w < len(elev):
            elev = uniform_filter1d(elev, size=w, mode="nearest")
            p(f"smoothed with window={smooth_window}m ({w} samples)")

        passes = [(False, lats, lngs, cum, elev)]
        if chain["bidirectional"]:
            passes.append((True, lats[::-1], lngs[::-1], cum[-1] - cum[::-1], elev[::-1]))

        for reversed_pass, p_lats, p_lngs, p_cum, p_elev in passes:
            direction = "reverse" if reversed_pass else "forward"
            p(f"\n--- {direction} ---")

            extrema = _find_extrema(p_elev, prominence)
            p(f"extrema (prominence={prominence}m): {len(extrema)}")
            for idx, kind in extrema:
                p(f"  [{idx:4d}] {kind:6s}  elev={p_elev[idx]:7.1f}m  "
                  f"dist={p_cum[idx]:8.1f}m  ({p_lats[idx]:.6f}, {p_lngs[idx]:.6f})")

            candidates = []
            for j in range(len(extrema) - 1):
                ti, ti_kind = extrema[j]
                pi, pi_kind = extrema[j + 1]
                if ti_kind != "trough" or pi_kind != "peak":
                    continue
                length = float(p_cum[pi] - p_cum[ti])
                gain = float(p_elev[pi] - p_elev[ti])
                if length <= 0 or gain <= 0:
                    continue
                grade = gain / length
                ok = length >= min_length and grade >= min_grade and gain >= min_gain
                reasons = []
                if length < min_length:
                    reasons.append(f"length {length:.0f}m < {min_length:.0f}m")
                if grade < min_grade:
                    reasons.append(f"grade {grade*100:.2f}% < {min_grade*100:.2f}%")
                if gain < min_gain:
                    reasons.append(f"gain {gain:.1f}m < {min_gain:.1f}m")
                flag = "OK  " if ok else "skip"
                suffix = f"  ({'; '.join(reasons)})" if reasons else ""
                p(f"  [{flag}] [{ti}→{pi}]  length={length:.0f}m  gain={gain:.1f}m  "
                  f"grade={grade*100:.2f}%{suffix}")
                if ok:
                    candidates.append((ti, pi, gain * grade))

            if not candidates:
                p("  no climbs found")
                continue

            candidates.sort(key=lambda c: -c[2])
            used = np.zeros(len(p_elev), dtype=bool)
            selected = []
            for ti, pi, _ in candidates:
                if used[ti : pi + 1].any():
                    p(f"  dropped [{ti}→{pi}] overlap")
                    continue
                used[ti : pi + 1] = True
                selected.append((ti, pi))
            selected.sort()
            p(f"  selected: {len(selected)} climb(s)")

            # 6. report each detected climb against the proto_climbs table
            for ti, pi in selected:
                nids, _ = _chain_node_slice(
                    list(node_cum), chain["way_per_coord"], chain["node_ids"],
                    float(node_cum[-1]), p_cum, ti, pi, reversed_pass,
                )
                p(f"    [{ti}→{pi}]")
                _show_proto_climb(conn, nids, max_strip, strip_degree)
    finally:
        conn.close()
