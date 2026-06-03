"""Deduplicate proto_climbs by Jaccard similarity of node sets."""
from __future__ import annotations

import logging

import psycopg2
from tqdm import tqdm

log = logging.getLogger(__name__)

_ITER_SIZE = 200


def _jaccard(a: set, b: set) -> float:
    union = len(a | b)
    return len(a & b) / union if union else 1.0


def dedupe_climbs(dsn: str, max_similarity: float) -> int:
    """Delete near-duplicate proto_climbs, keeping the one with more nodes.

    Two proto_climbs are duplicates when their Jaccard node-set similarity
    is >= max_similarity.  Returns the number of deleted rows.
    """
    read_conn = psycopg2.connect(dsn)
    write_conn = psycopg2.connect(dsn)

    to_delete: set[str] = set()

    try:
        with write_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM proto_climbs")
            total = cur.fetchone()[0]

        log.info("deduplicating %d proto_climbs (max_similarity=%.2f)", total, max_similarity)

        with read_conn.cursor(name="dedupe_stream") as cur:
            cur.itersize = _ITER_SIZE
            # Process longest climbs first so they survive deduplication.
            cur.execute(
                "SELECT id, nodes, start_lat, start_lng, distance "
                "FROM proto_climbs ORDER BY array_length(nodes, 1) DESC"
            )

            for row in tqdm(cur, total=total, desc="dedupe", unit="proto_climb"):
                pc_id = str(row[0])
                if pc_id in to_delete:
                    continue

                node_ids = list(row[1])
                start_lat, start_lng, distance = row[2], row[3], float(row[4])
                node_set = set(node_ids)
                radius = max(distance, 500.0)

                with write_conn.cursor() as wcur:
                    wcur.execute(
                        """
                        SELECT id, nodes
                        FROM proto_climbs
                        WHERE ST_DWithin(
                            ST_MakePoint(start_lng, start_lat)::geography,
                            ST_MakePoint(%s, %s)::geography,
                            %s
                        )
                        AND id != %s::uuid
                        """,
                        (start_lng, start_lat, radius, pc_id),
                    )
                    for r in wcur.fetchall():
                        other_id = str(r[0])
                        if other_id in to_delete:
                            continue
                        sim = _jaccard(node_set, set(r[1]))
                        if sim >= max_similarity:
                            to_delete.add(other_id)

        if to_delete:
            with write_conn.cursor() as wcur:
                wcur.execute(
                    "DELETE FROM proto_climbs WHERE id = ANY(%s::uuid[])",
                    (list(to_delete),),
                )
            write_conn.commit()
            log.info("deleted %d duplicate proto_climbs", len(to_delete))

    finally:
        read_conn.close()
        write_conn.close()

    return len(to_delete)
