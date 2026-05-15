import logging

import psycopg2.extensions

log = logging.getLogger(__name__)


def build_chains(conn: psycopg2.extensions.connection):
    with conn.cursor() as cur:
        cur.execute("UPDATE ways SET chain_id = id")
        conn.commit()

        # precompute endpoint nodes with exactly 2 ways (static throughout propagation)
        cur.execute("""
            CREATE TEMP TABLE chain_endpoints AS
            SELECT node_id FROM (
                SELECT nodes[1] AS node_id FROM ways
                UNION ALL
                SELECT nodes[array_length(nodes,1)] AS node_id FROM ways
            ) e
            GROUP BY node_id
            HAVING COUNT(*) = 2
        """)
        cur.execute("CREATE INDEX ON chain_endpoints (node_id)")
        conn.commit()

        iteration = 0
        while True:
            cur.execute("""
                UPDATE ways w
                SET chain_id = sub.min_chain
                FROM (
                    SELECT
                        unnest(ARRAY[w1.id, w2.id]) AS way_id,
                        LEAST(w1.chain_id, w2.chain_id) AS min_chain
                    FROM chain_endpoints ce
                    JOIN ways w1 ON w1.nodes[1] = ce.node_id
                                 OR w1.nodes[array_length(w1.nodes,1)] = ce.node_id
                    JOIN ways w2 ON (   w2.nodes[1] = ce.node_id
                                     OR w2.nodes[array_length(w2.nodes,1)] = ce.node_id)
                                 AND w1.id < w2.id
                                 AND w1.highway = w2.highway
                    WHERE w1.chain_id != w2.chain_id
                ) sub
                WHERE w.id = sub.way_id
                  AND w.chain_id > sub.min_chain
            """)
            affected = cur.rowcount
            conn.commit()
            iteration += 1
            log.info("chain propagation iteration %d: %d ways relabeled", iteration, affected)
            if affected == 0:
                break

        cur.execute("DROP TABLE chain_endpoints")
        conn.commit()


def chain_info(conn: psycopg2.extensions.connection) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT chain_length, COUNT(*) AS num_chains
            FROM (
                SELECT chain_id, COUNT(*) AS chain_length
                FROM ways
                WHERE chain_id IS NOT NULL
                GROUP BY chain_id
            ) sub
            GROUP BY chain_length
            ORDER BY chain_length
        """)
        distribution = {int(length): int(count) for length, count in cur.fetchall()}
    return {
        "total_chains": sum(distribution.values()),
        "length_distribution": distribution,
    }


def get_chain(conn: psycopg2.extensions.connection, chain_id: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, nodes[1], nodes[array_length(nodes,1)]
            FROM ways WHERE chain_id = %s
        """, (chain_id,))
        rows = cur.fetchall()

    if not rows:
        return []
    if len(rows) == 1:
        return [rows[0][0]]

    way_info: dict[int, tuple[int, int]] = {way_id: (start, end) for way_id, start, end in rows}
    end_to_way: dict[int, int] = {end: way_id for way_id, _, end in rows}
    start_to_way: dict[int, int] = {start: way_id for way_id, start, _ in rows}

    # head: a way whose start node is not the end of any other way in the chain
    head = next((way_id for way_id, start, _ in rows if start not in end_to_way), rows[0][0])

    ordered = []
    visited: set[int] = set()
    current: int | None = head
    while current and current not in visited:
        ordered.append(current)
        visited.add(current)
        current = start_to_way.get(way_info[current][1])

    return ordered
