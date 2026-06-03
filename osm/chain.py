import logging

import psycopg2.extensions

log = logging.getLogger(__name__)


def build_chains(conn: psycopg2.extensions.connection):
    with conn.cursor() as cur:
        cur.execute("UPDATE ways SET chain_id = id")
        conn.commit()

        # precompute (node_id, highway) pairs where exactly 2 ways of that highway
        # type share the endpoint — unambiguous continuation regardless of node degree
        cur.execute("""
            CREATE TEMP TABLE chain_endpoints AS
            SELECT node_id, highway FROM (
                SELECT nodes[1] AS node_id, highway FROM ways
                UNION ALL
                SELECT nodes[array_length(nodes,1)] AS node_id, highway FROM ways
            ) e
            GROUP BY node_id, highway
            HAVING COUNT(*) = 2
        """)
        cur.execute("CREATE INDEX ON chain_endpoints (node_id, highway)")
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
                    JOIN ways w1 ON (   w1.nodes[1] = ce.node_id
                                     OR w1.nodes[array_length(w1.nodes,1)] = ce.node_id)
                                 AND w1.highway = ce.highway
                    JOIN ways w2 ON (   w2.nodes[1] = ce.node_id
                                     OR w2.nodes[array_length(w2.nodes,1)] = ce.node_id)
                                 AND w2.highway = ce.highway
                                 AND w1.id < w2.id
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

    # Both endpoints for each way, direction-agnostic (mirrors how build_chains links ways)
    way_endpoints: dict[int, tuple[int, int]] = {way_id: (start, end) for way_id, start, end in rows}
    node_to_ways: dict[int, list[int]] = {}
    for way_id, start, end in rows:
        node_to_ways.setdefault(start, []).append(way_id)
        if end != start:
            node_to_ways.setdefault(end, []).append(way_id)

    # head: a way with a terminus endpoint (appears in only one chain way)
    head = exit_node = None
    for way_id, start, end in rows:
        if len(node_to_ways.get(start, [])) == 1:
            head, exit_node = way_id, end
            break
        if len(node_to_ways.get(end, [])) == 1:
            head, exit_node = way_id, start
            break
    if head is None:
        # cycle — arbitrary starting point
        head = rows[0][0]
        exit_node = rows[0][2]

    ordered: list[int] = [head]
    visited: set[int] = {head}
    while exit_node is not None:
        candidates = [w for w in node_to_ways.get(exit_node, []) if w not in visited]
        if not candidates:
            break
        nxt = candidates[0]
        ordered.append(nxt)
        visited.add(nxt)
        start, end = way_endpoints[nxt]
        other = {start, end} - {exit_node}
        exit_node = other.pop() if other else None

    return ordered
