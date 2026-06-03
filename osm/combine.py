"""Combine proto_climbs at junction nodes and insert combined results back into proto_climbs."""
from __future__ import annotations

import hashlib
import logging

import psycopg2
import psycopg2.extras
from pyproj import Geod
from tqdm import tqdm

log = logging.getLogger(__name__)

GEOD = Geod(ellps="WGS84")

_ITER_SIZE = 100


def _node_dist(a: tuple, b: tuple) -> float:
    _, _, d = GEOD.inv(a[1], a[0], b[1], b[0])
    return float(d) if d and d == d else 0.0


def _unique_in_order(items):
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def combine_climbs(dsn: str, max_combo: int) -> int:
    # Two connections: read_conn streams proto_climbs via server-side cursor;
    # write_conn handles all sub-queries and inserts so commits don't close the stream.
    read_conn = psycopg2.connect(dsn)
    write_conn = psycopg2.connect(dsn)

    node_cache: dict[int, tuple] = {}  # id -> (lat, lng, elevation, degree)
    emitted: set[tuple] = set()
    total_inserted = 0

    def _load_nodes(node_ids: list[int]) -> None:
        missing = [nid for nid in node_ids if nid not in node_cache]
        if not missing:
            return
        with write_conn.cursor() as cur:
            cur.execute(
                "SELECT id, lat, lng, COALESCE(elevation, 0.0), COALESCE(degree, 0) "
                "FROM nodes WHERE id = ANY(%s)",
                (missing,),
            )
            for r in cur:
                node_cache[r[0]] = (r[1], r[2], float(r[3]), int(r[4]))

    def _fetch_connected(nid: int, exclude: list[str], src_lat: float, src_lng: float, src_dist: float) -> list[dict]:
        with write_conn.cursor() as cur:
            cur.execute(
                """
                WITH geo_filtered AS MATERIALIZED (
                    SELECT id, nodes, osm_way_ids, start_lat, start_lng, distance
                    FROM proto_climbs
                    WHERE from_climbs IS NULL
                    AND ST_DWithin(
                        ST_MakePoint(start_lng, start_lat)::geography,
                        ST_MakePoint(%s, %s)::geography,
                        %s
                    )
                )
                SELECT id, nodes, osm_way_ids, start_lat, start_lng, distance
                FROM geo_filtered
                WHERE %s = ANY(nodes)
                AND id != ALL(%s::uuid[])
                """,
                (src_lng, src_lat, src_dist, nid, exclude),
            )
            result = []
            for r in cur.fetchall():
                other = {
                    "id": str(r[0]), "nodes": list(r[1]), "osm_way_ids": list(r[2]),
                    "start_lat": r[3], "start_lng": r[4], "distance": r[5],
                }
                _load_nodes(other["nodes"])
                result.append(other)
        return result

    def _build_node_list(
        path: list[dict], junctions: list[tuple[int, int]]
    ) -> tuple[list[int], list[int]]:
        """Build combined (node_ids, way_ids) for path+junctions.

        junctions[k] = (idx_in_path[k], idx_in_path[k+1]) — the junction node's
        position in the k-th and (k+1)-th climb respectively.
        """
        nodes: list[int] = []
        way_ids: list[int] = []

        c0 = path[0]
        end0 = junctions[0][0] + 1 if junctions else len(c0["nodes"])
        nodes.extend(c0["nodes"][:end0])
        way_ids.extend(c0["osm_way_ids"])

        for k in range(1, len(path) - 1):
            ck = path[k]
            nodes.extend(ck["nodes"][junctions[k - 1][1] + 1 : junctions[k][0] + 1])
            way_ids.extend(ck["osm_way_ids"])

        if len(path) > 1:
            cl = path[-1]
            nodes.extend(cl["nodes"][junctions[-1][1] + 1 :])
            way_ids.extend(cl["osm_way_ids"])

        return nodes, way_ids

    def _make_row(node_ids: list[int], way_ids: list[int], from_climbs: list[str]) -> dict | None:
        key = tuple(node_ids)
        if key in emitted or len(node_ids) < 2:
            return None
        emitted.add(key)

        valid = [nid for nid in node_ids if nid in node_cache]
        if len(valid) < 2:
            return None

        first = node_cache[valid[0]]

        dist = sum(
            _node_dist(node_cache[valid[i - 1]], node_cache[valid[i]])
            for i in range(1, len(valid))
        )
        if dist <= 0:
            return None

        nodes_hash = hashlib.md5(",".join(str(n) for n in sorted(node_ids)).encode()).hexdigest()

        return {
            "nodes": node_ids,
            "nodes_hash": nodes_hash,
            "osm_way_ids": _unique_in_order(way_ids),
            "start_lat": first[0],
            "start_lng": first[1],
            "distance": dist,
            "from_climbs": from_climbs,
        }

    def _insert_rows(rows: list[dict]) -> int:
        if not rows:
            return 0
        seen: dict[str, dict] = {}
        for r in rows:
            seen[r["nodes_hash"]] = r
        rows = list(seen.values())
        with write_conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO proto_climbs (nodes, nodes_hash, osm_way_ids, start_lat, start_lng, distance, from_climbs)
                VALUES %s
                ON CONFLICT (nodes_hash) DO NOTHING
                """,
                [
                    (
                        r["nodes"], r["nodes_hash"], r["osm_way_ids"],
                        r["start_lat"], r["start_lng"], r["distance"],
                        r["from_climbs"],
                    )
                    for r in rows
                ],
                template="(%s, %s, %s, %s, %s, %s, %s::uuid[])",
            )
        write_conn.commit()
        return len(rows)

    def _dfs(
        path: list[dict],
        junctions: list[tuple[int, int]],
        depth: int,
        out: list[dict],
    ) -> None:
        current = path[-1]
        entry_pos = junctions[-1][1] if junctions else -1
        exclude = [pc["id"] for pc in path]

        for i in range(entry_pos + 1, len(current["nodes"])):
            nid = current["nodes"][i]
            nd = node_cache.get(nid)
            if nd is None or nd[3] <= 2:
                continue
            if depth >= max_combo:
                continue

            for other in _fetch_connected(nid, exclude, current["start_lat"], current["start_lng"], current["distance"]):
                try:
                    j = other["nodes"].index(nid)
                except ValueError:
                    continue
                if j >= len(other["nodes"]) - 1:
                    continue

                new_path = path + [other]
                new_junctions = junctions + [(i, j)]
                node_ids, way_ids = _build_node_list(new_path, new_junctions)
                row = _make_row(node_ids, way_ids, [pc["id"] for pc in new_path])
                if row:
                    out.append(row)
                _dfs(new_path, new_junctions, depth + 1, out)

    try:
        with write_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM proto_climbs WHERE from_climbs IS NULL")
            total = cur.fetchone()[0]

        log.info("combining %d proto_climbs (max_combo=%d)", total, max_combo)

        with read_conn.cursor(name="pc_stream") as main_cur:
            main_cur.itersize = _ITER_SIZE
            main_cur.execute("SELECT id, nodes, osm_way_ids, start_lat, start_lng, distance FROM proto_climbs WHERE from_climbs IS NULL")

            for row in tqdm(main_cur, total=total, desc="combine", unit="proto_climb"):
                pc = {
                    "id": str(row[0]), "nodes": list(row[1]), "osm_way_ids": list(row[2]),
                    "start_lat": row[3], "start_lng": row[4], "distance": row[5],
                }
                _load_nodes(pc["nodes"])

                batch: list[dict] = []
                _dfs([pc], [], 1, batch)

                total_inserted += _insert_rows(batch)

    finally:
        read_conn.close()
        write_conn.close()

    return total_inserted
