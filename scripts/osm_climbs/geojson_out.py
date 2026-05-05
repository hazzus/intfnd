"""Optional GeoJSON dump for visual inspection of the detection output.

Used when --out-geojson is set. Each chain, climb and combination becomes a
LineString feature with mapnik-style stroke properties so the file can be
dropped straight onto geojson.io.
"""
import json

from .chains import Chain
from .detect import Climb, DetectedClimb

HIGHWAY_COLORS = {
    "primary": "#d62728",
    "primary_link": "#d62728",
    "secondary": "#ff7f0e",
    "secondary_link": "#ff7f0e",
    "tertiary": "#bcbd22",
    "tertiary_link": "#bcbd22",
    "unclassified": "#7f7f7f",
    "residential": "#9467bd",
    "road": "#7f7f7f",
    "cycleway": "#1f77b4",
    "track": "#8c564b",
    "living_street": "#17becf",
}


def chain_feature(chain: Chain) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": [[lng, lat] for lng, lat in chain.coords],
        },
        "properties": {
            "kind": "chain",
            "highway": chain.highway,
            "name": chain.name,
            "ref": chain.ref,
            "way_ids": chain.way_ids,
            "way_count": len(chain.way_ids),
            "stroke": HIGHWAY_COLORS.get(chain.highway, "#888888"),
            "stroke-width": 2,
            "stroke-opacity": 0.4,
        },
    }


def climb_feature(climb: Climb, chain: Chain) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            # climb.coords is (lat, lng); GeoJSON wants (lng, lat)
            "coordinates": [[lng, lat] for lat, lng in climb.coords],
        },
        "properties": {
            "kind": "climb",
            "highway": chain.highway,
            "name": chain.name,
            "ref": chain.ref,
            "way_ids": chain.way_ids,
            "length_m": round(climb.length_m, 1),
            "grade_pct": round(climb.grade * 100, 2),
            "gain_m": round(climb.gain_m, 1),
            "stroke": "#00b050",
            "stroke-width": 5,
            "stroke-opacity": 0.95,
        },
    }


def combination_feature(dc: DetectedClimb) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": [[lng, lat] for lat, lng in dc.climb.coords],
        },
        "properties": {
            "kind": "combination",
            "highway": dc.highway,
            "name": dc.name,
            "length_m": round(dc.climb.length_m, 1),
            "grade_pct": round(dc.climb.grade * 100, 2),
            "gain_m": round(dc.climb.gain_m, 1),
            "stroke": "#9b59b6",
            "stroke-width": 5,
            "stroke-opacity": 0.95,
        },
    }


def write_geojson(path: str, features: list[dict]) -> None:
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w") as f:
        json.dump(fc, f)
