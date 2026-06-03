"""Score proto_climbs and write them to climbs."""
from __future__ import annotations

import logging

import numpy as np
import polyline as polyline_lib
import psycopg2
import psycopg2.extras
from scipy.ndimage import uniform_filter1d
from scipy.signal import savgol_filter
from tqdm import tqdm

from geo import bearing, turn_angle, node_dist
from surface import get_surface

log = logging.getLogger(__name__)

_ITER_SIZE = 200
_BATCH_SIZE = 100

_SIGNAL_PENALTY     = 50.0

_HIGHWAY_RANK = {
    "primary": 5, "primary_link": 5,
    "secondary": 4, "secondary_link": 4,
    "tertiary": 3, "tertiary_link": 3,
    "unclassified": 2, "residential": 2,
    "living_street": 1, "road": 1,
    "cycleway": 0, "track": 0,
}
_INTERSECTION_SCALE = 6.0

_TURN_THRESHOLD = 20.0
_TURN_SCALE     = 0.2

_SPIKE_STEP      = 10.0   # resample interval in meters
_SPIKE_WINDOW    = 250.0  # smoothing window in meters
_SPIKE_THRESHOLD = 0.04   # grade deviation below which we ignore noise
_SPIKE_SCALE     = 1.0

_PROFILE_STEP   = 20.0   # uniform resample interval for stored profile (m)
_PROFILE_WINDOW = 400.0  # Savitzky-Golay window (m)
_PROFILE_ORDER  = 3      # polynomial order

_UNPAVED_TAGS = frozenset({
    "unpaved", "gravel", "dirt", "ground", "grass", "mud", "sand",
    "compacted", "fine_gravel", "pebblestone", "woodchips", "earth",
})


def _way_paved(surface: str | None, highway: str) -> bool:
    return get_surface(surface, highway) not in _UNPAVED_TAGS


def _resample(node_data: list[tuple], step: float) -> tuple[np.ndarray, float] | None:
    """Interpolate node elevations onto a uniform distance grid.

    Returns (uniform_elevations, total_distance), or None if the climb is too short.
    """
    if len(node_data) < 2:
        return None
    cum = [0.0]
    for i in range(1, len(node_data)):
        cum.append(cum[-1] + node_dist(
            node_data[i-1][0], node_data[i-1][1],
            node_data[i][0],   node_data[i][1],
        ))
    total = cum[-1]
    if total < step * 2:
        return None
    elevs   = np.array([n[2] for n in node_data], dtype=float)
    cum_arr = np.array(cum, dtype=float)
    n_pts        = max(3, int(total / step))
    uniform_cum  = np.linspace(0.0, total, n_pts)
    uniform_elev = np.interp(uniform_cum, cum_arr, elevs)
    return uniform_elev, total


def _spike_penalty(node_data: list[tuple]) -> float:
    """Compute gradient spike penalty from node (lat, lng, elevation) tuples."""
    result = _resample(node_data, _SPIKE_STEP)
    if result is None:
        return 0.0
    uniform_elev, total = result

    n_pts    = len(uniform_elev)
    step     = total / (n_pts - 1)
    raw_grad = np.diff(uniform_elev) / step

    window     = max(1, int(round(_SPIKE_WINDOW / step)))
    smooth_grad = uniform_filter1d(raw_grad, size=window, mode="nearest")

    dev    = np.abs(raw_grad - smooth_grad)
    excess = np.maximum(0.0, dev - _SPIKE_THRESHOLD)
    return float(np.sum(excess)) * _SPIKE_SCALE


def _smooth_elevation_profile(node_data: list[tuple]) -> list[float]:
    """Resample to a uniform distance grid and apply Savitzky-Golay smoothing."""
    result = _resample(node_data, _PROFILE_STEP)
    if result is None:
        return [n[2] for n in node_data]
    uniform_elev, _ = result

    n_pts  = len(uniform_elev)
    window = int(round(_PROFILE_WINDOW / _PROFILE_STEP))
    window = window + (1 - window % 2)                 # must be odd
    window = min(window, n_pts - (1 - n_pts % 2))     # can't exceed n_pts (keep odd)
    if window <= _PROFILE_ORDER:                       # too few points to smooth
        return uniform_elev.tolist()
    window = max(window, _PROFILE_ORDER + 2)           # savgol minimum

    smoothed = savgol_filter(uniform_elev, window_length=window, polyorder=_PROFILE_ORDER)
    return smoothed.tolist()


def _load_nodes(conn, node_ids: list[int], node_cache: dict[int, tuple]) -> None:
    missing = [nid for nid in node_ids if nid not in node_cache]
    if not missing:
        return
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, lat, lng, COALESCE(elevation, 0.0), "
            "COALESCE(is_signal, FALSE), COALESCE(degree, 0) "
            "FROM nodes WHERE id = ANY(%s)",
            (missing,),
        )
        for r in cur:
            node_cache[r[0]] = (float(r[1]), float(r[2]), float(r[3]), bool(r[4]), int(r[5]))


def _load_ways(conn, way_ids: list[int], way_cache: dict[int, tuple]) -> None:
    missing = [wid for wid in way_ids if wid not in way_cache]
    if not missing:
        return
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, COALESCE(name, ''), COALESCE(ref, ''), highway, surface, bidirectional "
            "FROM ways WHERE id = ANY(%s)",
            (missing,),
        )
        for r in cur:
            way_cache[r[0]] = (r[1], r[2], r[3], r[4], bool(r[5]))


def _load_crossings(
    conn, node_ids: list[int], climb_way_ids: list[int],
    node_cache: dict[int, tuple], crossing_cache: dict[int, int],
) -> None:
    to_query = [
        nid for nid in node_ids
        if nid not in crossing_cache and node_cache.get(nid, (0, 0, 0, False, 0))[4] > 2
    ]
    if not to_query:
        return
    with conn.cursor() as cur:
        cur.execute(
            "SELECT n.id, w.highway "
            "FROM unnest(%s::bigint[]) AS n(id) "
            "JOIN ways w ON n.id = ANY(w.nodes) "
            "WHERE w.id != ALL(%s::bigint[])",
            (to_query, climb_way_ids),
        )
        results: dict[int, int] = {}
        for nid, hw in cur:
            rank = _HIGHWAY_RANK.get(hw, 0)
            if rank > results.get(nid, 0):
                results[nid] = rank
    for nid in to_query:
        crossing_cache[nid] = results.get(nid, 0)


def _score_climb(
    node_ids: list[int], way_ids: list[int],
    nodes: list[tuple], ways: list[tuple],
    distance: float, start_lat: float, start_lng: float,
    crossing_cache: dict[int, int],
) -> tuple[tuple, dict]:
    """Score one resolved proto_climb. Returns (climbs row, score breakdown)."""
    average_grade = 100.0 * ((nodes[-1][2] - nodes[0][2]) / distance if distance > 0 else 0.0)
    elevation_profile = _smooth_elevation_profile(nodes)
    encoded = polyline_lib.encode([(n[0], n[1]) for n in nodes])
    end_lat, end_lng = nodes[-1][0], nodes[-1][1]
    surfaces = list(dict.fromkeys(w[3] for w in ways if w[3]))
    is_paved = all(_way_paved(w[3], w[2]) for w in ways) if ways else True
    name = next((w[0] for w in ways if w[0]), next((w[1] for w in ways if w[1]), ""))
    bidirectional = bool(ways) and all(w[4] for w in ways)

    # Signals
    n_signals = sum(1 for n in nodes if n[3])
    signal_penalty = n_signals * _SIGNAL_PENALTY

    # Intersections
    intersection_penalty = 0.0
    n_intersections = 0
    for nid, n in zip(node_ids, nodes):
        if n[4] > 2:
            rank = crossing_cache.get(nid, 0)
            if rank:
                n_intersections += 1
            intersection_penalty += rank * _INTERSECTION_SCALE

    # Sharp turns — only penalized when the vertex is an intersection
    turn_penalty = 0.0
    turns: list[tuple[float, float, float, float]] = []  # (lat, lng, angle, penalty)
    for i in range(1, len(nodes) - 1):
        prev, cur_n, nxt = nodes[i - 1], nodes[i], nodes[i + 1]
        if cur_n[4] <= 2:
            continue
        b1 = bearing(prev[0], prev[1], cur_n[0], cur_n[1])
        b2 = bearing(cur_n[0], cur_n[1], nxt[0],  nxt[1])
        angle = turn_angle(b1, b2)
        if angle >= _TURN_THRESHOLD:
            pen = angle * _TURN_SCALE
            turn_penalty += pen
            turns.append((cur_n[0], cur_n[1], angle, pen))

    # Gradient spikes
    spike_penalty = _spike_penalty(nodes)

    score = signal_penalty + intersection_penalty + turn_penalty + spike_penalty

    row = (
        name, distance, average_grade,
        start_lat, start_lng, end_lat, end_lng,
        encoded, surfaces, is_paved,
        elevation_profile, way_ids,
        bidirectional, score,
    )
    breakdown = {
        "name": name, "distance": distance, "average_grade": average_grade,
        "end_lat": end_lat, "end_lng": end_lng,
        "surfaces": surfaces, "is_paved": is_paved, "bidirectional": bidirectional,
        "n_nodes": len(nodes), "n_profile": len(elevation_profile), "polyline_len": len(encoded),
        "n_signals": n_signals, "signal_penalty": signal_penalty,
        "n_intersections": n_intersections, "intersection_penalty": intersection_penalty,
        "n_sharp_turns": len(turns), "turn_penalty": turn_penalty, "turns": turns,
        "spike_penalty": spike_penalty,
        "score": score,
    }
    return row, breakdown


def score_proto(
    conn, node_ids: list[int], way_ids: list[int],
    start_lat: float, start_lng: float, distance: float,
) -> tuple[tuple, dict] | None:
    """One-shot scoring of a single proto_climb (used by the debugger).

    Returns (climbs row, score breakdown), or None if nodes are missing.
    """
    node_ids = list(node_ids)
    way_ids = list(way_ids)
    node_cache: dict[int, tuple] = {}
    way_cache: dict[int, tuple] = {}
    crossing_cache: dict[int, int] = {}
    _load_nodes(conn, node_ids, node_cache)
    _load_ways(conn, way_ids, way_cache)
    _load_crossings(conn, node_ids, way_ids, node_cache, crossing_cache)
    if not node_ids or any(nid not in node_cache for nid in node_ids):
        return None
    nodes = [node_cache[nid] for nid in node_ids]
    ways = [way_cache[wid] for wid in way_ids if wid in way_cache]
    return _score_climb(node_ids, way_ids, nodes, ways, distance, start_lat, start_lng, crossing_cache)


def score_climbs(dsn: str) -> int:
    """Score each proto_climb and insert into climbs. Returns number inserted."""
    read_conn = psycopg2.connect(dsn)
    write_conn = psycopg2.connect(dsn)

    node_cache: dict[int, tuple] = {}      # id -> (lat, lng, elevation, is_signal, degree)
    way_cache: dict[int, tuple] = {}        # id -> (name, ref, highway, surface, bidirectional)
    crossing_cache: dict[int, int] = {}     # node_id -> max highway rank of crossing ways

    def _insert_batch(rows: list[tuple]) -> int:
        if not rows:
            return 0
        with write_conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO climbs (
                    name, distance, average_grade,
                    start_lat, start_lng, end_lat, end_lng,
                    polyline, surfaces, is_paved,
                    elevation_profile, osm_way_ids,
                    bidirectional, score
                ) VALUES %s
                ON CONFLICT (start_lat, start_lng, end_lat, end_lng, osm_way_ids) DO UPDATE SET
                    name              = EXCLUDED.name,
                    distance          = EXCLUDED.distance,
                    average_grade     = EXCLUDED.average_grade,
                    polyline          = EXCLUDED.polyline,
                    surfaces          = EXCLUDED.surfaces,
                    is_paved          = EXCLUDED.is_paved,
                    elevation_profile = EXCLUDED.elevation_profile,
                    bidirectional     = EXCLUDED.bidirectional,
                    score             = EXCLUDED.score
                """,
                rows,
            )
        write_conn.commit()
        return cur.rowcount

    total_inserted = 0
    batch: dict[tuple, tuple] = {}

    try:
        with write_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM proto_climbs")
            total = cur.fetchone()[0]

        log.info("scoring %d proto_climbs", total)

        with read_conn.cursor(name="score_stream") as cur:
            cur.itersize = _ITER_SIZE
            cur.execute(
                "SELECT id, nodes, osm_way_ids, start_lat, start_lng, distance FROM proto_climbs"
            )

            for row in tqdm(cur, total=total, desc="score", unit="proto_climb"):
                pc_id, node_ids, way_ids, start_lat, start_lng, distance = row
                node_ids = list(node_ids)
                way_ids = list(way_ids)

                _load_nodes(write_conn, node_ids, node_cache)
                _load_ways(write_conn, way_ids, way_cache)
                _load_crossings(write_conn, node_ids, way_ids, node_cache, crossing_cache)

                if any(nid not in node_cache for nid in node_ids):
                    log.debug("skipping proto_climb %s: missing nodes", pc_id)
                    continue
                if len(node_ids) == 0:
                    log.debug("skipping empty climbs %s", pc_id)
                    continue

                nodes = [node_cache[nid] for nid in node_ids]
                ways = [way_cache[wid] for wid in way_ids if wid in way_cache]
                row, _ = _score_climb(
                    node_ids, way_ids, nodes, ways, distance, start_lat, start_lng, crossing_cache,
                )

                key = (row[3], row[4], row[5], row[6], tuple(way_ids))
                batch[key] = row

                if len(batch) >= _BATCH_SIZE:
                    total_inserted += _insert_batch(list(batch.values()))
                    batch.clear()

        total_inserted += _insert_batch(list(batch.values()))

    finally:
        read_conn.close()
        write_conn.close()

    return total_inserted
