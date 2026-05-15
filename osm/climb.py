import logging
import math
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from tqdm import tqdm
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from pyproj import Geod
from scipy.ndimage import uniform_filter1d
from scipy.signal import find_peaks

from chain import get_chain

log = logging.getLogger(__name__)

_CHUNKS_PER_WORKER = 128

GEOD = Geod(ellps="WGS84")


def _resample(coords: list[tuple[float, float]], step_m: float):
    if len(coords) < 2:
        return None
    lats = [coords[0][1]]
    lngs = [coords[0][0]]
    cum = [0.0]
    total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1]
        lng2, lat2 = coords[i]
        az, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if dist == 0 or not np.isfinite(dist):
            continue
        n = max(1, int(np.floor(dist / step_m)))
        for k in range(1, n + 1):
            d = dist * (k / n)
            lo, la, _ = GEOD.fwd(lng1, lat1, az, d)
            lats.append(la)
            lngs.append(lo)
            cum.append(total + d)
        total += dist
    if len(lats) < 2:
        return None
    return np.array(lats), np.array(lngs), np.array(cum)


def _cumulative_distances(coords: list[tuple[float, float]]) -> list[float]:
    cum = [0.0]
    total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1]
        lng2, lat2 = coords[i]
        _, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if np.isfinite(dist):
            total += dist
        cum.append(total)
    return cum


def _find_extrema(elev: np.ndarray, prominence: float) -> list[tuple[int, str]]:
    n = len(elev)
    if n < 2:
        return []
    peaks, _ = find_peaks(elev, prominence=prominence)
    troughs, _ = find_peaks(-elev, prominence=prominence)
    extrema = sorted([(int(i), "trough") for i in troughs] + [(int(i), "peak") for i in peaks])
    if extrema:
        first_idx, _ = extrema[0]
        start_kind = "trough" if elev[0] <= elev[first_idx] else "peak"
        last_idx, _ = extrema[-1]
        end_kind = "trough" if elev[-1] <= elev[last_idx] else "peak"
    else:
        start_kind, end_kind = ("trough", "peak") if elev[0] <= elev[-1] else ("peak", "trough")
    if not extrema or extrema[0][0] != 0:
        extrema.insert(0, (0, start_kind))
    if extrema[-1][0] != n - 1:
        extrema.append((n - 1, end_kind))
    return extrema


def _unique_in_order(items):
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _chain_node_slice(
    chain_cum, way_per_coord, node_ids,
    chain_total, p_cum, ti, pi, reversed_pass,
):
    start_d = float(p_cum[ti])
    end_d = float(p_cum[pi])
    if reversed_pass:
        s = chain_total - end_d
        e = chain_total - start_d
    else:
        s, e = start_d, end_d
    nids, wids = [], []
    for nid, wid, cd in zip(node_ids, way_per_coord, chain_cum):
        if s <= cd <= e:
            nids.append(nid)
            wids.append(wid)
    if reversed_pass:
        nids.reverse()
        wids.reverse()
    return nids, wids


def _detect_in_pass(lats, lngs, cum, elev, min_length, min_grade, min_gain, prominence):
    if len(elev) < 3:
        return []
    extrema = _find_extrema(elev, prominence)
    if len(extrema) < 2:
        return []

    candidates = []
    for j in range(len(extrema) - 1):
        ti, ti_kind = extrema[j]
        pi, pi_kind = extrema[j + 1]
        if ti_kind != "trough" or pi_kind != "peak":
            continue
        length = float(cum[pi] - cum[ti])
        gain = float(elev[pi] - elev[ti])
        if length <= 0 or gain <= 0:
            continue
        grade = gain / length
        if length >= min_length and grade >= min_grade and gain >= min_gain:
            candidates.append((ti, pi, length, grade, gain, gain * grade))

    candidates.sort(key=lambda c: -c[5])
    used = np.zeros(len(elev), dtype=bool)
    selected = []
    for ti, pi, length, grade, gain, _ in candidates:
        if used[ti : pi + 1].any():
            continue
        used[ti : pi + 1] = True
        selected.append((ti, pi))
    selected.sort()

    return [
        (ti, pi, float(lats[ti]), float(lngs[ti]), float(cum[pi] - cum[ti]))
        for ti, pi in selected
    ]


def _build_chain(conn: psycopg2.extensions.connection, chain_id: int):
    way_ids = get_chain(conn, chain_id)
    if not way_ids:
        return None

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, highway, surface, nodes, bidirectional FROM ways WHERE id = ANY(%s)",
            (way_ids,),
        )
        way_map = {r[0]: r for r in cur.fetchall()}

    node_id_set: set[int] = set()
    for wid in way_ids:
        if wid not in way_map:
            return None
        node_id_set.update(way_map[wid][3])

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, lat, lng, elevation FROM nodes WHERE id = ANY(%s)",
            (list(node_id_set),),
        )
        node_map = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

    for nid in node_id_set:
        if nid not in node_map or node_map[nid][2] is None:
            return None

    coords: list[tuple[float, float]] = []
    elevations: list[float] = []
    way_per_coord: list[int] = []
    node_ids: list[int] = []

    prev_end: int | None = None
    for wid in way_ids:
        _, _, _, nodes, _ = way_map[wid]
        if prev_end is not None:
            if nodes[0] == prev_end:
                seg = nodes
            elif nodes[-1] == prev_end:
                seg = nodes[::-1]
            else:
                return None
        else:
            seg = nodes

        start = 1 if coords else 0
        for nid in seg[start:]:
            lat, lng, elev = node_map[nid]
            coords.append((lng, lat))
            elevations.append(float(elev))
            way_per_coord.append(wid)
            node_ids.append(nid)

        prev_end = seg[-1]

    highway = way_map[way_ids[0]][1]
    bidirectional = all(way_map[wid][4] for wid in way_ids)
    is_cycle = len(coords) >= 3 and coords[0] == coords[-1]

    return {
        "coords": coords,
        "elevations": elevations,
        "way_per_coord": way_per_coord,
        "node_ids": node_ids,
        "highway": highway,
        "bidirectional": bidirectional,
        "is_cycle": is_cycle,
    }


def _rotate_cycle(chain: dict) -> dict:
    coords = chain["coords"]
    elevations = chain["elevations"]
    way_per = chain["way_per_coord"]
    node_ids = chain["node_ids"]

    interior_coords = coords[:-1]
    interior_elevs = elevations[:-1]
    interior_way = way_per[:-1]
    interior_nids = node_ids[:-1]

    k = int(np.argmin(interior_elevs))
    if k == 0:
        return chain

    return {
        **chain,
        "coords":        interior_coords[k:] + interior_coords[:k] + [interior_coords[k]],
        "elevations":    interior_elevs[k:]  + interior_elevs[:k]  + [interior_elevs[k]],
        "way_per_coord": interior_way[k:]    + interior_way[:k]    + [interior_way[k]],
        "node_ids":      interior_nids[k:]   + interior_nids[:k]   + [interior_nids[k]],
    }


def _detect_chain_climbs(
    chain: dict,
    sample_step: float,
    smooth_window: float,
    min_length: float,
    min_grade: float,
    min_gain: float,
    prominence: float,
) -> list[dict]:
    coords = chain["coords"]
    elevations = np.array(chain["elevations"], dtype=float)

    resampled = _resample(coords, sample_step)
    if resampled is None:
        return []
    lats, lngs, cum = resampled
    if cum[-1] < min_length:
        return []

    node_cum = np.array(_cumulative_distances(coords))
    elev = np.interp(cum, node_cum, elevations)

    w = max(1, int(round(smooth_window / sample_step)))
    if 1 < w < len(elev):
        elev = uniform_filter1d(elev, size=w, mode="nearest")

    chain_cum = _cumulative_distances(coords)
    chain_total = chain_cum[-1] if chain_cum else 0.0

    passes: list[tuple] = [(False, lats, lngs, cum, elev)]
    if chain["bidirectional"]:
        passes.append((True, lats[::-1], lngs[::-1], cum[-1] - cum[::-1], elev[::-1]))

    rows = []
    for reversed_pass, p_lats, p_lngs, p_cum, p_elev in passes:
        for ti, pi, start_lat, start_lng, distance in _detect_in_pass(
            p_lats, p_lngs, p_cum, p_elev, min_length, min_grade, min_gain, prominence,
        ):
            nids, wids = _chain_node_slice(
                chain_cum, chain["way_per_coord"], chain["node_ids"],
                chain_total, p_cum, ti, pi, reversed_pass,
            )
            rows.append({
                "nodes": nids,
                "osm_way_ids": _unique_in_order(wids),
                "start_lat": start_lat,
                "start_lng": start_lng,
                "distance": distance,
            })
    return rows


def _insert_batch(conn: psycopg2.extensions.connection, rows: list[dict]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO proto_climbs (nodes, osm_way_ids, start_lat, start_lng, distance)
            VALUES %s
            ON CONFLICT (md5(nodes::text)) DO UPDATE SET
                osm_way_ids = EXCLUDED.osm_way_ids,
                start_lat   = EXCLUDED.start_lat,
                start_lng   = EXCLUDED.start_lng,
                distance    = EXCLUDED.distance
            """,
            [
                (r["nodes"], r["osm_way_ids"], r["start_lat"], r["start_lng"], r["distance"])
                for r in rows
            ],
        )
    conn.commit()


def _worker_batch(args: tuple) -> tuple[int, int]:
    dsn, chain_ids, params = args
    conn = psycopg2.connect(dsn)
    try:
        total = skipped = 0
        for chain_id in chain_ids:
            chain = _build_chain(conn, chain_id)
            if chain is None:
                skipped += 1
                continue
            if chain["is_cycle"]:
                chain = _rotate_cycle(chain)
            rows = _detect_chain_climbs(chain, **params)
            if rows:
                _insert_batch(conn, rows)
                total += len(rows)
        return total, skipped
    finally:
        conn.close()


def process_climbs(
    dsn: str,
    sample_step: float = 10.0,
    smooth_window: float = 100.0,
    min_length: float = 300.0,
    min_grade: float = 0.01,
    min_gain: float = 0.0,
    prominence: float = 10.0,
    workers: int = 1,
) -> None:
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT chain_id FROM ways WHERE chain_id IS NOT NULL ORDER BY chain_id"
        )
        chain_ids = [r[0] for r in cur.fetchall()]
    conn.close()

    log.info("processing %d chains", len(chain_ids))

    params = dict(
        sample_step=sample_step,
        smooth_window=smooth_window,
        min_length=min_length,
        min_grade=min_grade,
        min_gain=min_gain,
        prominence=prominence,
    )
    total = skipped = 0

    if workers <= 1:
        conn = psycopg2.connect(dsn)
        for chain_id in tqdm(chain_ids, unit="chain"):
            chain = _build_chain(conn, chain_id)
            if chain is None:
                skipped += 1
                continue
            if chain["is_cycle"]:
                chain = _rotate_cycle(chain)
            rows = _detect_chain_climbs(chain, **params)
            if rows:
                _insert_batch(conn, rows)
                total += len(rows)
        conn.close()
    else:
        chunk_size = max(1, math.ceil(len(chain_ids) / (workers * _CHUNKS_PER_WORKER)))
        log.info("Chunk size: %d", chunk_size)
        chunks = [chain_ids[i:i + chunk_size] for i in range(0, len(chain_ids), chunk_size)]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_worker_batch, (dsn, chunk, params)) for chunk in chunks]
            for f in tqdm(as_completed(futures), total=len(futures), unit="chunk"):
                t, s = f.result()
                total += t
                skipped += s

    log.info("inserted %d proto_climbs; skipped %d chains", total, skipped)


def debug_chain(
    dsn: str,
    chain_id: int,
    sample_step: float = 10.0,
    smooth_window: float = 100.0,
    min_length: float = 300.0,
    min_grade: float = 0.01,
    min_gain: float = 0.0,
    prominence: float = 10.0,
) -> None:
    p = print
    p(f"\n=== chain {chain_id} ===")

    conn = psycopg2.connect(dsn)
    try:
        chain = _build_chain(conn, chain_id)
    finally:
        conn.close()

    if chain is None:
        p("FAIL: _build_chain returned None (missing elevation or disconnected ways)")
        return

    coords = chain["coords"]
    elevations = chain["elevations"]
    p(f"coords: {len(coords)}  is_cycle={chain['is_cycle']}  bidirectional={chain['bidirectional']}  highway={chain['highway']!r}")
    p(f"elevation range: {min(elevations):.1f}m – {max(elevations):.1f}m")

    if chain["is_cycle"]:
        chain = _rotate_cycle(chain)
        elevations = chain["elevations"]
        p(f"rotated cycle: elevation now {elevations[0]:.1f}m – {elevations[-1]:.1f}m")

    resampled = _resample(coords, sample_step)
    if resampled is None:
        p("FAIL: _resample returned None")
        return
    lats, lngs, cum = resampled
    chain_total = cum[-1]
    p(f"resampled: {len(lats)} points  total={chain_total:.1f}m  step={sample_step}m")

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
            p(f"  [{idx:4d}] {kind:6s}  elev={p_elev[idx]:7.1f}m  dist={p_cum[idx]:8.1f}m  ({p_lats[idx]:.6f}, {p_lngs[idx]:.6f})")

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
            p(f"  [{flag}] [{ti}→{pi}]  length={length:.0f}m  gain={gain:.1f}m  grade={grade*100:.2f}%{suffix}")
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
