import json
import logging

import osmium
import psycopg2.extensions
import psycopg2.extras

from surface import get_surface


def _insert_ways(conn: psycopg2.extensions.connection, batch: list):
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO ways (id, name, ref, highway, surface, nodes, bidirectional, tags) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                ref = EXCLUDED.ref,
                highway = EXCLUDED.highway,
                surface = EXCLUDED.surface,
                nodes = EXCLUDED.nodes,
                bidirectional = EXCLUDED.bidirectional,
                tags = EXCLUDED.tags
            """,
            batch,
            template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
        )
    conn.commit()


def _insert_nodes(conn: psycopg2.extensions.connection, batch: list):
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO nodes (id, lat, lng, is_signal, tags) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                is_signal = EXCLUDED.is_signal,
                tags = EXCLUDED.tags
            """,
            batch,
        )
    conn.commit()

log = logging.getLogger(__name__)

CYCLABLE_HIGHWAYS = {
    "primary", "primary_link",
    "secondary", "secondary_link",
    "tertiary", "tertiary_link",
    "unclassified", "residential", "road",
    "cycleway", "track", "living_street",
}

_BATCH_SIZE = 1000


class WayCollector(osmium.SimpleHandler):
    def __init__(self, postgres: psycopg2.extensions.connection):
        super().__init__()
        self.postgres = postgres
        self.interesting_nodes = set()
        self._batch = []
        self._total = 0

    def way(self, w):
        highway = w.tags.get("highway")
        if highway not in CYCLABLE_HIGHWAYS:
            return
        if w.tags.get("bicycle") in ("no", "private"):
            return
        if w.tags.get("tunnel") in ("yes", "true", "1"):
            return

        tags = {k: v for k, v in w.tags}
        oneway = tags.get("oneway")
        oneway_bicycle = tags.get("oneway:bicycle")
        is_oneway = oneway in ("yes", "1", "true", "-1") and oneway_bicycle != "no"

        node_ids = [n.ref for n in w.nodes]
        if oneway == "-1":
            node_ids.reverse()
        self.interesting_nodes.update(node_ids)
        self._batch.append((
            w.id,
            tags.get("name"),
            tags.get("ref"),
            highway,
            get_surface(tags.get("surface"), highway),
            node_ids,
            not is_oneway,
            json.dumps(tags),
        ))

        if len(self._batch) >= _BATCH_SIZE:
            self._flush()

    def _flush(self):
        if not self._batch:
            return
        _insert_ways(self.postgres, self._batch)
        self._total += len(self._batch)
        self._batch = []


class NodeCollector(osmium.SimpleHandler):
    def __init__(self, postgres: psycopg2.extensions.connection, interesting_nodes: set[int]):
        super().__init__()
        self.postgres = postgres
        self.interesting_nodes = interesting_nodes
        self._batch = []
        self._total = 0

    def node(self, n):
        if n.id not in self.interesting_nodes:
            return

        tags = {k: v for k, v in n.tags}
        self._batch.append((
            n.id,
            float(n.location.lat),
            float(n.location.lon),
            n.tags.get("highway") == "traffic_signals",
            json.dumps(tags),
        ))

        if len(self._batch) >= _BATCH_SIZE:
            self._flush()

    def _flush(self):
        if not self._batch:
            return
        _insert_nodes(self.postgres, self._batch)
        self._total += len(self._batch)
        self._batch = []


def load_data(file: str, postgres: psycopg2.extensions.connection):
    log.info("loading ways from %s", file)
    way_collector = WayCollector(postgres)
    way_collector.apply_file(file)
    way_collector._flush()
    log.info("ways done: %d total, %d unique nodes to resolve", way_collector._total, len(way_collector.interesting_nodes))

    log.info("loading nodes from %s", file)
    node_collector = NodeCollector(postgres, way_collector.interesting_nodes)
    node_collector.apply_file(file)
    node_collector._flush()
    log.info("nodes done: %d total", node_collector._total)
