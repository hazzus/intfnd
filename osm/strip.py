"""Strip sharp-turn approaches from the start/end of proto_climbs."""
from __future__ import annotations

import hashlib
import logging

import psycopg2
import psycopg2.extensions
from tqdm import tqdm

from geo import bearing as _bearing, turn_angle as _turn_angle, node_dist as _node_dist

log = logging.getLogger(__name__)

_ITER_SIZE = 500


def _nodes_hash(nids: list[int]) -> str:
    return hashlib.md5(",".join(str(n) for n in sorted(nids)).encode()).hexdigest()


def _strip_ends(
    nodes_latlon: list[tuple[float, float]],
    max_strip_m: float,
    strip_degree: float,
) -> tuple[int, int]:
    """Return (start_idx, end_idx) open-ended slice to keep after stripping.

    Walks the first max_strip_m from the start and last max_strip_m from the end.
    At each interior node the bearing change is measured; if it exceeds strip_degree
    the strip boundary is advanced past that node.  Returns (0, n) when nothing needs
    to change or stripping would leave fewer than 2 nodes.
    """
    n = len(nodes_latlon)
    if n < 3:
        return 0, n

    cum = [0.0]
    for i in range(1, n):
        lat0, lng0 = nodes_latlon[i - 1]
        lat1, lng1 = nodes_latlon[i]
        cum.append(cum[-1] + _node_dist(lat0, lng0, lat1, lng1))

    total = cum[-1]

    # Strip from start: keep advancing past each sharp turn within max_strip_m.
    start_idx = 0
    for i in range(1, n - 1):
        if cum[i] > max_strip_m:
            break
        lat0, lng0 = nodes_latlon[i - 1]
        lat1, lng1 = nodes_latlon[i]
        lat2, lng2 = nodes_latlon[i + 1]
        if _turn_angle(_bearing(lat0, lng0, lat1, lng1), _bearing(lat1, lng1, lat2, lng2)) >= strip_degree:
            start_idx = i + 1

    # Strip from end: keep advancing past each sharp turn within max_strip_m (walking backward).
    end_idx = n
    for i in range(n - 2, 0, -1):
        if total - cum[i] > max_strip_m:
            break
        lat0, lng0 = nodes_latlon[i - 1]
        lat1, lng1 = nodes_latlon[i]
        lat2, lng2 = nodes_latlon[i + 1]
        if _turn_angle(_bearing(lat0, lng0, lat1, lng1), _bearing(lat1, lng1, lat2, lng2)) >= strip_degree:
            end_idx = i

    if start_idx >= end_idx or end_idx - start_idx < 2:
        return 0, n

    return start_idx, end_idx


def strip_climbs(dsn: str, max_strip_m: float, strip_degree: float) -> int:
    """Strip sharp-turn ends from all proto_climbs in place.

    Uses two connections: read_conn streams proto_climbs via server-side cursor;
    write_conn handles node lookups and updates so commits don't close the stream.
    Returns the number of proto_climbs that were modified.
    """
    read_conn = psycopg2.connect(dsn)
    write_conn = psycopg2.connect(dsn)

    node_cache: dict[int, tuple[float, float]] = {}  # id -> (lat, lng)
    updated = 0

    def _load_nodes(node_ids: list[int]) -> None:
        missing = [nid for nid in node_ids if nid not in node_cache]
        if not missing:
            return
        with write_conn.cursor() as cur:
            cur.execute("SELECT id, lat, lng FROM nodes WHERE id = ANY(%s)", (missing,))
            for r in cur:
                node_cache[r[0]] = (float(r[1]), float(r[2]))

    try:
        with write_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM proto_climbs")
            total = cur.fetchone()[0]

        log.info(
            "stripping %d proto_climbs (max_strip=%.1fm, strip_degree=%.1f°)",
            total, max_strip_m, strip_degree,
        )

        with read_conn.cursor(name="strip_stream") as cur:
            cur.itersize = _ITER_SIZE
            cur.execute("SELECT id, nodes FROM proto_climbs")

            for row in tqdm(cur, total=total, desc="strip", unit="proto_climb"):
                pc_id, node_ids = row
                node_ids = list(node_ids)

                _load_nodes(node_ids)

                nodes_latlon = [node_cache[nid] for nid in node_ids if nid in node_cache]
                if len(nodes_latlon) != len(node_ids):
                    continue

                s, e = _strip_ends(nodes_latlon, max_strip_m, strip_degree)
                if s == 0 and e == len(node_ids):
                    continue

                new_ids = node_ids[s:e]
                new_latlon = nodes_latlon[s:e]
                new_dist = sum(
                    _node_dist(*new_latlon[i - 1], *new_latlon[i])
                    for i in range(1, len(new_latlon))
                )
                new_hash = _nodes_hash(new_ids)

                try:
                    with write_conn.cursor() as wcur:
                        wcur.execute(
                            """
                            UPDATE proto_climbs
                            SET nodes      = %s,
                                nodes_hash = %s,
                                start_lat  = %s,
                                start_lng  = %s,
                                distance   = %s
                            WHERE id = %s
                              AND NOT EXISTS (
                                  SELECT 1 FROM proto_climbs
                                  WHERE nodes_hash = %s AND id != %s
                              )
                            """,
                            (
                                new_ids, new_hash,
                                new_latlon[0][0], new_latlon[0][1],
                                new_dist,
                                pc_id,
                                new_hash, pc_id,
                            ),
                        )
                    write_conn.commit()
                    if wcur.rowcount:
                        updated += 1
                except Exception:
                    write_conn.rollback()
                    log.debug("skipped proto_climb %s (hash conflict after strip)", pc_id)

    finally:
        read_conn.close()
        write_conn.close()

    log.info("stripped %d / %d proto_climbs", updated, total)
    return updated
