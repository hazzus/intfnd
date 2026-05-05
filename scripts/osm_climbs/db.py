"""Row construction and Postgres upsert for the climbs table."""
import polyline as polyline_lib
import psycopg

from .detect import DetectedClimb
from .surface import ASPHALT_SURFACES


def to_climb_row(dc: DetectedClimb) -> dict:
    return {
        "name": dc.name,
        "distance": float(dc.climb.length_m),
        "average_grade": float(dc.climb.grade) * 100,
        "start_lat": float(dc.climb.coords[0][0]),
        "start_lng": float(dc.climb.coords[0][1]),
        "polyline": polyline_lib.encode(dc.climb.coords),
        "surfaces": list(dc.surfaces),
        "is_paved": bool(all(map(lambda s: s in ASPHALT_SURFACES, dc.surfaces))),
        "elevation_profile": [float(x) for x in dc.elevation_profile],
        "osm_way_ids": [int(x) for x in dc.osm_way_ids],
        "bidirectional": bool(dc.bidirectional),
        "score": float(dc.score),
    }


def insert_climbs(rows: list[dict], dsn: str) -> None:
    if not rows:
        return
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO climbs
                    (name, distance, average_grade, start_lat, start_lng, polyline,
                     surfaces, elevation_profile, osm_way_ids, bidirectional, score, is_paved)
                VALUES
                    (%(name)s, %(distance)s, %(average_grade)s,
                     %(start_lat)s, %(start_lng)s, %(polyline)s,
                     %(surfaces)s, %(elevation_profile)s, %(osm_way_ids)s,
                     %(bidirectional)s, %(score)s, %(is_paved)s)
                ON CONFLICT (start_lat, start_lng, osm_way_ids)
                DO UPDATE SET
                    name = %(name)s,
                    distance = %(distance)s,
                    polyline = %(polyline)s,
                    surfaces = %(surfaces)s,
                    elevation_profile = %(elevation_profile)s,
                    bidirectional = %(bidirectional)s,
                    score = %(score)s,
                    is_paved = %(is_paved)s
                """,
                rows,
            )
        conn.commit()
