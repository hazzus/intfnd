import logging

import psycopg2.extensions
from tqdm import tqdm

log = logging.getLogger(__name__)

_BATCH_SIZE = 50_000


def fill_node_degrees(conn: psycopg2.extensions.connection):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE tmp_node_degree (node_id BIGINT PRIMARY KEY, cnt INT NOT NULL DEFAULT 0)
        """)
        cur.execute("SELECT id FROM ways ORDER BY id")
        way_ids = [r[0] for r in cur.fetchall()]

    batches = range(0, len(way_ids), _BATCH_SIZE)
    for i in tqdm(batches, unit="batch", desc="degrees"):
        batch = way_ids[i : i + _BATCH_SIZE]
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tmp_node_degree (node_id, cnt)
                SELECT unnest(nodes) AS node_id, COUNT(*) AS cnt
                FROM ways
                WHERE id = ANY(%s)
                GROUP BY node_id
                ON CONFLICT (node_id) DO UPDATE SET cnt = tmp_node_degree.cnt + EXCLUDED.cnt
            """, (batch,))
        conn.commit()

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE nodes n
            SET degree = t.cnt
            FROM tmp_node_degree t
            WHERE n.id = t.node_id
        """)
    conn.commit()


def degree_distribution(conn: psycopg2.extensions.connection) -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT degree, COUNT(*) FROM nodes WHERE degree IS NOT NULL GROUP BY degree ORDER BY degree")
        return {degree: count for degree, count in cur.fetchall()}
