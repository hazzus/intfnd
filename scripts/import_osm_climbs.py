#!/usr/bin/env python3
"""Extract climbs from OSM PBF + DEM and insert into the climbs table.

Not idempotent: re-running on the same input will create duplicate rows.

Example:
    python import_osm_climbs.py \\
        --pbf liechtenstein.osm.pbf \\
        --dem liechtenstein-dem.tif \\
        --db postgres://postgres:pw@localhost/intfnd

"""
import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import numpy as np
import osmium
import polyline as polyline_lib
import psycopg
import rasterio
from pyproj import Geod
from scipy.ndimage import uniform_filter1d
from scipy.signal import find_peaks
from tqdm import tqdm

GEOD = Geod(ellps="WGS84")
log = logging.getLogger("osm_climbs")

CYCLABLE_HIGHWAYS = {
    "primary", "primary_link",
    "secondary", "secondary_link",
    "tertiary", "tertiary_link",
    "unclassified", "residential", "road",
    "cycleway", "track", "living_street",
}

ASPHALT_SURFACES = {
    "asphalt", "paved", "concrete", "concrete:lanes", "concrete:plates",
    "paving_stones", "chipseal", "metal",
}
NON_ASPHALT_SURFACES = {
    "gravel", "fine_gravel", "dirt", "ground", "earth", "unpaved",
    "sand", "mud", "grass", "compacted", "wood", "woodchips",
    "pebblestone", "cobblestone", "sett",
}
HIGHWAY_DEFAULT_ASPHALT = {
    "primary", "primary_link", "secondary", "secondary_link",
    "tertiary", "tertiary_link", "unclassified", "residential",
    "living_street", "road",
}


def classify_surface(tags: dict, highway: str) -> str:
    """Return 'asphalt' or 'non_asphalt' for a way."""
    s = (tags.get("surface") or "").lower()
    if s in ASPHALT_SURFACES:
        return "asphalt"
    if s in NON_ASPHALT_SURFACES:
        return "non_asphalt"
    # No (or unrecognized) surface tag — fall back to highway class.
    return "asphalt" if highway in HIGHWAY_DEFAULT_ASPHALT else "non_asphalt"


@dataclass
class Climb:
    coords: list[tuple[float, float]]
    length_m: float
    grade: float
    gain_m: float


@dataclass
class DetectedClimb:
    """A climb produced by the detector, with the provenance needed to combine it with others."""
    climb: Climb
    name: str
    surfaces: list[str]
    highway: str
    osm_way_ids: list[int]
    bidirectional: bool
    elevation_profile: list[float]
    # Original (lng, lat) OSM nodes covering the climb, ordered ascending (low → high).
    # Used both for chaining into bigger combinations and for node-overlap deduplication.
    nodes: list[tuple[float, float]]
    node_way_ids: list[int]
    node_surfaces: list[str]
    is_combination: bool = False
    score: float = 0.0


def unique_in_order(items):
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


@dataclass
class Way:
    id: int
    coords: list[tuple[float, float]]  # (lng, lat)
    name: str | None
    ref: str | None
    highway: str
    surface: str  # 'asphalt' or 'non_asphalt'
    tags: dict


class WayCollector(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.ways: list[Way] = []

    def way(self, w):
        tags = w.tags
        hw = tags.get("highway")
        if hw not in CYCLABLE_HIGHWAYS:
            return
        if tags.get("bicycle") == "no":
            return
        # Tunnels: DEM samples ground surface, not the tunnel floor, so elevation is bogus.
        tunnel = tags.get("tunnel")
        if tunnel and tunnel != "no":
            return
        try:
            coords = [(n.lon, n.lat) for n in w.nodes if n.location.valid()]
        except osmium.InvalidLocationError:
            return
        if len(coords) < 2:
            return
        tag_dict = {t.k: t.v for t in tags}
        self.ways.append(
            Way(
                id=w.id,
                coords=coords,
                name=tags.get("name"),
                ref=tags.get("ref"),
                highway=hw or "<none>",
                surface=classify_surface(tag_dict, hw or ""),
                tags=tag_dict,
            )
        )


def load_ways(pbf_path: str) -> list[Way]:
    handler = WayCollector()
    # locations=True caches node coords so they're attached to ways
    handler.apply_file(pbf_path, locations=True)
    return handler.ways


@dataclass
class Chain:
    way_ids: list[int]
    coords: list[tuple[float, float]]  # (lng, lat) deduped at way boundaries
    coord_way_ids: list[int]  # parallel to coords: which way each coord came from
    coord_surfaces: list[str]  # parallel to coords: resolved surface class per coord
    name: str | None
    ref: str | None
    highway: str
    surface: str  # 'asphalt' or 'non_asphalt'
    bidirectional: bool
    tags: dict = field(default_factory=dict)

    @property
    def primary_id(self) -> int:
        return self.way_ids[0]


def build_chains(ways: list[Way]) -> list[Chain]:
    """Stitch ways into chains across shared endpoint nodes.

    At a node, ports are grouped by highway type; two ports are paired only when
    their highway-group at that node has exactly two ports (from different ways).
    This means a primary road continuing through a 3-way junction where a side
    street of a different class branches off will still stitch — but a 4-way
    crossing of two primaries will not, since the pairing is ambiguous.
    Ways with start == end (closed loops) are not stitched.
    """
    if not ways:
        return []

    # node coord -> [(way_idx, 'start'|'end')]
    endpoints: dict[tuple[float, float], list[tuple[int, str]]] = defaultdict(list)
    for i, w in enumerate(ways):
        if w.coords[0] == w.coords[-1]:
            continue  # closed loop: skip endpoint registration so it stays a chain by itself
        endpoints[w.coords[0]].append((i, "start"))
        endpoints[w.coords[-1]].append((i, "end"))

    # partner[(way, port)] = (other_way, other_port) when the node has exactly
    # two ports with the same highway type (regardless of total node degree).
    partner: dict[tuple[int, str], tuple[int, str]] = {}
    for ports in endpoints.values():
        if len(ports) < 2:
            continue
        by_hw: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for idx, port in ports:
            by_hw[ways[idx].highway].append((idx, port))
        for group in by_hw.values():
            if len(group) != 2:
                continue
            (i1, p1), (i2, p2) = group
            if i1 == i2:
                continue
            partner[(i1, p1)] = (i2, p2)
            partner[(i2, p2)] = (i1, p1)

    visited = [False] * len(ways)

    def walk(seed_way: int, seed_port: str, direction: str) -> list[tuple[int, bool]]:
        """Walk through partners. Returns [(way_idx, reversed_flag), ...] in walk order."""
        out: list[tuple[int, bool]] = []
        cur_way, cur_port = seed_way, seed_port
        while (cur_way, cur_port) in partner:
            next_way, next_port = partner[(cur_way, cur_port)]
            if visited[next_way]:
                break
            visited[next_way] = True
            if direction == "forward":
                rev = next_port == "end"
            else:
                rev = next_port == "start"
            out.append((next_way, rev))
            cur_way = next_way
            cur_port = "start" if next_port == "end" else "end"
        return out

    chains: list[Chain] = []
    for seed in range(len(ways)):
        if visited[seed]:
            continue
        visited[seed] = True
        forward = walk(seed, "end", "forward")
        backward = walk(seed, "start", "backward")
        ordered: list[tuple[int, bool]] = (
            list(reversed(backward)) + [(seed, False)] + forward
        )

        # Decide whether the chain's stitched order is rideable as-is, only in reverse,
        # both ways, or not at all. For each member, the chain's "forward" traversal
        # consumes the way in 'forward' direction when rev=False, else in 'reverse'.
        chain_fwd_ok = True
        chain_rev_ok = True
        for way_idx, rev in ordered:
            dirs = way_directions(ways[way_idx].tags)
            fwd_member_dir = "reverse" if rev else "forward"
            rev_member_dir = "forward" if rev else "reverse"
            if fwd_member_dir not in dirs:
                chain_fwd_ok = False
            if rev_member_dir not in dirs:
                chain_rev_ok = False
            if not chain_fwd_ok and not chain_rev_ok:
                break

        if not chain_fwd_ok and not chain_rev_ok:
            # Members disagree on direction — chain isn't rideable end-to-end.
            continue

        if not chain_fwd_ok and chain_rev_ok:
            # Flip the chain so its stored "forward" matches the legal direction.
            ordered = [(idx, not rev) for idx, rev in reversed(ordered)]

        # Concatenate coords, deduping the shared boundary node between adjacent ways.
        combined: list[tuple[float, float]] = []
        combined_way_ids: list[int] = []
        combined_surfaces: list[str] = []
        for k, (way_idx, rev) in enumerate(ordered):
            seg = ways[way_idx].coords if not rev else list(reversed(ways[way_idx].coords))
            wid = ways[way_idx].id
            surf = ways[way_idx].surface
            if k == 0:
                combined.extend(seg)
                combined_way_ids.extend([wid] * len(seg))
                combined_surfaces.extend([surf] * len(seg))
            else:
                combined.extend(seg[1:])
                combined_way_ids.extend([wid] * (len(seg) - 1))
                combined_surfaces.extend([surf] * (len(seg) - 1))

        members = [ways[i] for i, _ in ordered]
        highway = Counter(m.highway for m in members).most_common(1)[0][0]
        # Strict: any non-asphalt member taints the chain (riders care about the worst patch).
        surface = "non_asphalt" if any(m.surface == "non_asphalt" for m in members) else "asphalt"
        names = [m.name for m in members if m.name]
        seed_way = ways[seed]
        chains.append(
            Chain(
                way_ids=[ways[i].id for i, _ in ordered],
                coords=combined,
                coord_way_ids=combined_way_ids,
                coord_surfaces=combined_surfaces,
                name=seed_way.name or (names[0] if names else None),
                ref=seed_way.ref,
                highway=highway,
                surface=surface,
                bidirectional=chain_fwd_ok and chain_rev_ok,
                tags=seed_way.tags,
            )
        )
    return chains


def resample_way(coords, step_m: float):
    """coords: shapely LineString.coords as [(lng, lat), ...]. Returns (lats, lngs, cum)."""
    if len(coords) < 2:
        return None
    lats = [coords[0][1]]
    lngs = [coords[0][0]]
    cum = [0.0]
    cum_total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1][0], coords[i - 1][1]
        lng2, lat2 = coords[i][0], coords[i][1]
        az, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if dist == 0 or not np.isfinite(dist):
            continue
        n = max(1, int(np.floor(dist / step_m)))
        for k in range(1, n + 1):
            d = dist * (k / n)
            lon_k, lat_k, _ = GEOD.fwd(lng1, lat1, az, d)
            lats.append(lat_k)
            lngs.append(lon_k)
            cum.append(cum_total + d)
        cum_total += dist
    if len(lats) < 2:
        return None
    return np.array(lats), np.array(lngs), np.array(cum)


def sample_elevation(dataset, lats: np.ndarray, lngs: np.ndarray) -> np.ndarray:
    pts = list(zip(lngs.tolist(), lats.tolist()))
    samples = list(dataset.sample(pts))
    elev = np.array([s[0] for s in samples], dtype=float)
    nodata = dataset.nodata
    if nodata is not None:
        elev[elev == nodata] = np.nan
    elev[elev < -1000] = np.nan
    return elev


def smooth(elev: np.ndarray, window_m: float, step_m: float) -> np.ndarray:
    w = max(1, int(round(window_m / step_m)))
    if w <= 1 or w >= len(elev):
        return elev
    return uniform_filter1d(elev, size=w, mode="nearest")


def find_extrema_with_boundary(elev: np.ndarray, prominence: float) -> list[tuple[int, str]]:
    """Ordered list of (idx, 'trough'|'peak'), including synthetic endpoints at 0 and len-1.

    The endpoints are classified relative to the nearest interior extremum so that monotonic
    profiles still produce a single trough→peak (or peak→trough) pair.
    """
    n = len(elev)
    if n < 2:
        return []
    peaks, _ = find_peaks(elev, prominence=prominence)
    troughs, _ = find_peaks(-elev, prominence=prominence)
    extrema = sorted(
        [(int(i), "trough") for i in troughs] + [(int(i), "peak") for i in peaks]
    )
    if extrema:
        first_idx, _ = extrema[0]
        start_kind = "trough" if elev[0] <= elev[first_idx] else "peak"
        last_idx, _ = extrema[-1]
        end_kind = "trough" if elev[-1] <= elev[last_idx] else "peak"
    else:
        start_kind, end_kind = ("trough", "peak") if elev[0] <= elev[-1] else ("peak", "trough")
    if not extrema or extrema[0][0] != 0:
        extrema.insert(0, (0, start_kind))
    if extrema[-1][0] != n - 1:
        extrema.append((n - 1, end_kind))
    return extrema


def detect_climbs(
    lats: np.ndarray,
    lngs: np.ndarray,
    cum: np.ndarray,
    elev: np.ndarray,
    min_length: float,
    min_grade: float,
    min_gain: float,
    prominence: float,
) -> list[tuple[Climb, int, int]]:
    if len(elev) < 3:
        return []

    extrema = find_extrema_with_boundary(elev, prominence)
    if len(extrema) < 2:
        return []

    candidates = []
    for j in range(len(extrema) - 1):
        i_idx, i_kind = extrema[j]
        k_idx, k_kind = extrema[j + 1]
        if i_kind != "trough" or k_kind != "peak":
            continue
        length = float(cum[k_idx] - cum[i_idx])
        gain = float(elev[k_idx] - elev[i_idx])
        if length <= 0:
            continue
        grade = gain / length
        if grade <= 0:
            continue
        if length >= min_length and grade >= min_grade and gain >= min_gain:
            score = gain * grade
            candidates.append((i_idx, k_idx, length, grade, gain, score))

    candidates.sort(key=lambda c: -c[5])
    used = np.zeros(len(elev), dtype=bool)
    selected = []
    for ti, pi, length, grade, gain, _ in candidates:
        if used[ti : pi + 1].any():
            continue
        used[ti : pi + 1] = True
        selected.append((ti, pi, length, grade, gain))
    selected.sort()

    climbs: list[tuple[Climb, int, int]] = []
    for ti, pi, length, grade, gain in selected:
        coords = list(zip(lats[ti : pi + 1].tolist(), lngs[ti : pi + 1].tolist()))
        climbs.append((Climb(coords=coords, length_m=length, grade=grade, gain_m=gain), ti, pi))
    return climbs


def way_directions(tags: dict) -> set[str]:
    """Set of legal travel directions for a cyclist, relative to the way's stored coord order.

    'forward' = traversing coords[0] → coords[-1].
    'reverse' = traversing coords[-1] → coords[0].
    """
    bike = tags.get("oneway:bicycle")
    if bike == "no":
        return {"forward", "reverse"}
    if bike == "yes":
        return {"forward"}
    if bike == "-1":
        return {"reverse"}
    oneway = tags.get("oneway")
    if oneway in ("yes", "true", "1"):
        return {"forward"}
    if oneway in ("-1", "reverse"):
        return {"reverse"}
    return {"forward", "reverse"}


def reverse_profile(lats, lngs, cum, elev):
    return (
        lats[::-1],
        lngs[::-1],
        cum[-1] - cum[::-1],
        elev[::-1],
    )


def chain_display_name(chain: Chain) -> str:
    if chain.name:
        return str(chain.name)
    if chain.ref:
        return f"Climb on {chain.ref}"
    return "Unnamed climb"


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


def way_length_m(coords: list[tuple[float, float]]) -> float:
    total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1]
        lng2, lat2 = coords[i]
        _, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if np.isfinite(dist):
            total += dist
    return total


def rotate_loop_chain(chain: Chain, dem) -> Chain:
    """If chain is a closed loop, return it rotated so coords[0] is at the lowest-elevation node.

    Loops have coords[0] == coords[-1] (the polyline closes on the same OSM node). The
    detector treats coords[0] as a synthetic trough; if that boundary happens to fall
    mid-climb, the detector misses the true ascent extent. Rotating to the lowest node
    makes the synthetic trough coincide with the real loop low point.
    """
    if len(chain.coords) < 3 or chain.coords[0] != chain.coords[-1]:
        return chain
    interior = chain.coords[:-1]
    interior_wids = chain.coord_way_ids[:-1]
    interior_surfs = chain.coord_surfaces[:-1]
    lngs = np.array([c[0] for c in interior])
    lats = np.array([c[1] for c in interior])
    elev = sample_elevation(dem, lats, lngs)
    if np.any(np.isnan(elev)):
        return chain
    k = int(np.argmin(elev))
    if k == 0:
        return chain
    rotated = list(interior[k:]) + list(interior[:k])
    rotated.append(rotated[0])
    rotated_wids = list(interior_wids[k:]) + list(interior_wids[:k])
    rotated_wids.append(rotated_wids[0])
    rotated_surfs = list(interior_surfs[k:]) + list(interior_surfs[:k])
    rotated_surfs.append(rotated_surfs[0])
    return Chain(
        way_ids=chain.way_ids,
        coords=rotated,
        coord_way_ids=rotated_wids,
        coord_surfaces=rotated_surfs,
        name=chain.name,
        ref=chain.ref,
        highway=chain.highway,
        surface=chain.surface,
        bidirectional=chain.bidirectional,
        tags=chain.tags,
    )


def compute_node_degree(ways: list[Way]) -> dict[tuple[float, float], int]:
    """Number of distinct cyclable ways touching each node coord.

    Degree >= 3 is treated as a real intersection by the scorer; degree 2 is just a
    chain continuation. OSM normally splits ways at junctions, so junction nodes
    surface here as the endpoints shared between multiple ways.
    """
    node_ways: dict[tuple[float, float], set[int]] = defaultdict(set)
    for w in ways:
        for c in w.coords:
            node_ways[c].add(w.id)
    return {k: len(v) for k, v in node_ways.items()}


def bearing(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Forward azimuth from a to b in degrees, normalised to [0, 360)."""
    az, _, _ = GEOD.inv(a[0], a[1], b[0], b[1])
    return az % 360.0


def compute_score(
    nodes: list[tuple[float, float]],
    elevation_profile: list[float],
    length_m: float,
    sample_step: float,
    node_degree: dict[tuple[float, float], int],
) -> float:
    """Climb quality score in [0, 1]; lower is better.

    Penalises (a) intersection density along the climb, (b) average turn sharpness at
    those intersections, and (c) point-to-point grade variability on the smoothed
    elevation profile. Weights 0.4 / 0.3 / 0.3.
    """
    if length_m <= 0 or len(nodes) < 2:
        return 1.0

    intersections = 0
    turn_sum_deg = 0.0
    for i in range(1, len(nodes) - 1):
        if node_degree.get(nodes[i], 0) < 3:
            continue
        intersections += 1
        d = abs(bearing(nodes[i], nodes[i + 1]) - bearing(nodes[i - 1], nodes[i])) % 360.0
        if d > 180.0:
            d = 360.0 - d
        turn_sum_deg += d

    length_km = max(length_m / 1000.0, 0.1)
    inter_score = min(1.0, (intersections / length_km) / 4.0)
    turn_score = min(1.0, (turn_sum_deg / intersections) / 90.0) if intersections else 0.0

    if len(elevation_profile) >= 3 and sample_step > 0:
        step_grade = np.diff(np.asarray(elevation_profile, dtype=float)) / sample_step
        spike_score = min(1.0, float(np.std(step_grade)) / 0.05)
    else:
        spike_score = 0.0

    return float(0.4 * inter_score + 0.3 * turn_score + 0.3 * spike_score)


def cumulative_distances(coords: list[tuple[float, float]]) -> list[float]:
    """Cumulative geodesic distance along (lng, lat) coords; parallel to coords."""
    cum = [0.0]
    total = 0.0
    for i in range(1, len(coords)):
        lng1, lat1 = coords[i - 1]
        lng2, lat2 = coords[i]
        _, _, dist = GEOD.inv(lng1, lat1, lng2, lat2)
        if np.isfinite(dist):
            total += dist
        cum.append(total)
    return cum


def chain_node_slice(
    chain: Chain,
    chain_cum: list[float],
    chain_total: float,
    p_cum: np.ndarray,
    ti: int,
    pi: int,
    reversed_pass: bool,
) -> tuple[list[tuple[float, float]], list[int], list[str]]:
    """Original chain nodes (lng, lat) covering a climb's resampled range, ordered ascending,
    along with the parallel way_id and resolved surface for each node.

    For the reverse pass, the resampled cumulative distances are measured from the
    chain's end, so we mirror them back into chain-forward space and then reverse
    the slice so it still goes low-elevation → high-elevation.
    """
    start_d = float(p_cum[ti])
    end_d = float(p_cum[pi])
    if reversed_pass:
        s = chain_total - end_d
        e = chain_total - start_d
    else:
        s, e = start_d, end_d
    nodes: list[tuple[float, float]] = []
    wids: list[int] = []
    surfs: list[str] = []
    for c, wid, surf, cd in zip(chain.coords, chain.coord_way_ids, chain.coord_surfaces, chain_cum):
        if s <= cd <= e:
            nodes.append(c)
            wids.append(wid)
            surfs.append(surf)
    if reversed_pass:
        nodes.reverse()
        wids.reverse()
        surfs.reverse()
    return nodes, wids, surfs


def combo_name(names: list[str]) -> str:
    return " + ".join(unique_in_order(names))


def build_combinations(
    detected: list[DetectedClimb],
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
    geojson_features: list[dict] | None = None,
) -> list[DetectedClimb]:
    """For each ordered sequence of up to --max-combo climbs sharing OSM nodes at consecutive
    junctions, build C0_start → N1 → ... → Nk-1 → Ck_end and re-run the climb detector on the
    joined polyline.

    Combinations are explicitly allowed to cross highway types — that's the whole point,
    since the chain stitcher refuses to merge across highway-class transitions.

    Walks the climb-adjacency graph by DFS up to depth max_combo. At depth k there are k-1
    junctions; we require the detected climb to straddle all of them, so a k-deep combo that
    only spans some junctions is dropped (it was already emitted at a shallower depth).
    """
    if not detected:
        return []

    max_len = max(2, int(args.max_combo))

    # Index nodes → (climb_idx, position_in_climb).
    node_map: dict[tuple[float, float], list[tuple[int, int]]] = defaultdict(list)
    for ci, dc in enumerate(detected):
        for pos, node in enumerate(dc.nodes):
            node_map[node].append((ci, pos))

    processed_paths: set[tuple[int, ...]] = set()
    out: list[DetectedClimb] = []

    def emit(path: list[int], splits: list[tuple[int, int]]) -> None:
        first = detected[path[0]]
        first_p = splits[0][0]
        combined_nodes: list[tuple[float, float]] = list(first.nodes[: first_p + 1])
        combined_node_way_ids: list[int] = list(first.node_way_ids[: first_p + 1])
        combined_node_surfaces: list[str] = list(first.node_surfaces[: first_p + 1])
        junction_indices = [first_p]
        for i in range(1, len(path)):
            mid = detected[path[i]]
            q_prev = splits[i - 1][1]
            seg_end = splits[i][0] + 1 if i < len(path) - 1 else len(mid.nodes)
            combined_nodes.extend(mid.nodes[q_prev + 1 : seg_end])
            combined_node_way_ids.extend(mid.node_way_ids[q_prev + 1 : seg_end])
            combined_node_surfaces.extend(mid.node_surfaces[q_prev + 1 : seg_end])
            if i < len(path) - 1:
                junction_indices.append(len(combined_nodes) - 1)

        if len(combined_nodes) < 2:
            return
        combined_node_cum = cumulative_distances(combined_nodes)
        if combined_node_cum[-1] < args.min_length:
            return
        junction_dists = [combined_node_cum[idx] for idx in junction_indices]

        resampled = resample_way(combined_nodes, args.sample_step)
        if resampled is None:
            return
        lats, lngs, cum = resampled
        elev = sample_elevation(dem, lats, lngs)
        if np.any(np.isnan(elev)):
            return
        elev = smooth(elev, args.smooth_window, args.sample_step)

        members = [detected[ci] for ci in path]
        unique_highways = unique_in_order([m.highway for m in members])
        highway = unique_highways[0] if len(unique_highways) == 1 else "+".join(unique_highways)
        name = combo_name([m.name for m in members])

        for climb, ti, pi in detect_climbs(
            lats, lngs, cum, elev,
            args.min_length, args.min_grade, args.min_gain, args.prominence,
        ):
            start_d = float(cum[ti])
            end_d = float(cum[pi])
            # The detected climb must straddle every junction on the path; otherwise the
            # detector just rediscovered a shorter combination already emitted at a
            # shallower DFS depth (or a single climb in isolation).
            if not all(start_d <= jd <= end_d for jd in junction_dists):
                continue
            climb_nodes: list[tuple[float, float]] = []
            climb_wids: list[int] = []
            climb_surfs: list[str] = []
            for cn, cw, cs, cd in zip(
                combined_nodes, combined_node_way_ids, combined_node_surfaces, combined_node_cum
            ):
                if start_d <= cd <= end_d:
                    climb_nodes.append(cn)
                    climb_wids.append(cw)
                    climb_surfs.append(cs)
            elevation_profile = [float(x) for x in elev[ti : pi + 1]]
            dc = DetectedClimb(
                climb=climb,
                name=name,
                surfaces=unique_in_order(climb_surfs),
                highway=highway,
                osm_way_ids=unique_in_order(climb_wids),
                bidirectional=False,
                elevation_profile=elevation_profile,
                nodes=climb_nodes,
                node_way_ids=climb_wids,
                node_surfaces=climb_surfs,
                is_combination=True,
                score=compute_score(
                    climb_nodes, elevation_profile, climb.length_m,
                    args.sample_step, node_degree,
                ),
            )
            out.append(dc)
            if geojson_features is not None:
                geojson_features.append(combination_feature(dc))

    def extend(path: list[int], splits: list[tuple[int, int]], current_q: int) -> None:
        if len(path) >= max_len:
            return
        curr = detected[path[-1]]
        # Exit positions strictly after where we entered the current climb, so each
        # member contributes at least one node to the combined polyline.
        for p in range(current_q + 1, len(curr.nodes)):
            for cj, qj in node_map[curr.nodes[p]]:
                if cj in path:
                    continue
                # Need real tail past the junction in the next climb.
                if qj >= len(detected[cj].nodes) - 1:
                    continue
                new_path = path + [cj]
                key = tuple(new_path)
                if key in processed_paths:
                    continue
                processed_paths.add(key)
                new_splits = splits + [(p, qj)]
                emit(new_path, new_splits)
                extend(new_path, new_splits, qj)

    for ci in range(len(detected)):
        extend([ci], [], 0)

    return out


def debug_chain(chain: Chain, ways_by_id: dict[int, Way], dem, args) -> None:
    print(f"\n=== chain (seed way {chain.primary_id}, {len(chain.way_ids)} way(s)) ===")
    print(f"  way_ids: {chain.way_ids}")
    print(f"  highway: {chain.highway}   name: {chain.name!r}   ref: {chain.ref!r}")
    print(f"  bidirectional: {chain.bidirectional}")
    print(f"  raw coords: {len(chain.coords)} nodes")

    # Surface verdict breakdown: a chain is non_asphalt if ANY member resolves to non_asphalt.
    print(f"  surface verdict: {chain.surface}")
    print(f"  per-member surface (tag → resolved):")
    member_lengths = [way_length_m(ways_by_id[wid].coords) for wid in chain.way_ids]
    chain_total = sum(member_lengths) or 1.0
    tainters: list[tuple[int, float, str, str, str]] = []
    for wid, length_m in zip(chain.way_ids, member_lengths):
        w = ways_by_id[wid]
        raw_tag = w.tags.get("surface") or "<none>"
        marker = "  ← TAINTS" if w.surface == "non_asphalt" else ""
        pct = 100 * length_m / chain_total
        print(
            f"    way/{wid:>11}  len={length_m:7.1f}m ({pct:5.1f}%)  "
            f"highway={w.highway:<14}  surface={raw_tag:<14}  → {w.surface}{marker}"
        )
        if w.surface == "non_asphalt":
            tainters.append((wid, length_m, w.highway, raw_tag, w.surface))
    if chain.surface == "non_asphalt":
        share = 100 * sum(t[1] for t in tainters) / chain_total
        print(f"  → marked non_asphalt because {len(tainters)}/{len(chain.way_ids)} member(s) "
              f"({share:.1f}% of length) resolved to non_asphalt")
        for wid, length_m, hw, raw_tag, _ in tainters:
            reason = (
                f"explicit surface={raw_tag}" if raw_tag != "<none>"
                else f"no surface tag, highway={hw} defaults to non_asphalt"
            )
            print(f"      way/{wid}: {reason}")

    resampled = resample_way(chain.coords, args.sample_step)
    if resampled is None:
        print("  RESAMPLE FAILED (way too short or zero-length climbs)")
        return
    lats, lngs, cum = resampled
    print(f"  after {args.sample_step}m resample: {len(lats)} points, total length {cum[-1]:.1f} m")

    if cum[-1] < args.min_length:
        print(f"  REJECTED: total length {cum[-1]:.1f} m < --min-length {args.min_length}")
        return

    elev_raw = sample_elevation(dem, lats, lngs)
    nan_count = int(np.isnan(elev_raw).sum())
    if nan_count:
        print(f"  REJECTED: {nan_count} of {len(elev_raw)} elevation samples are nodata/NaN")
        print("  → DEM probably doesn't cover this way's bbox")
        return

    elev = smooth(elev_raw, args.smooth_window, args.sample_step)
    print(f"  elevation: raw min={elev_raw.min():.1f} max={elev_raw.max():.1f}  "
          f"smoothed min={elev.min():.1f} max={elev.max():.1f}  "
          f"raw gain={elev_raw.max()-elev_raw.min():.1f} m")

    passes = [("forward", lats, lngs, cum, elev)]
    if chain.bidirectional:
        rl, rln, rc, re = reverse_profile(lats, lngs, cum, elev)
        passes.append(("reverse", rl, rln, rc, re))

    accepted_per_pass: list[tuple[str, list]] = []
    for label, p_lats, p_lngs, p_cum, p_elev in passes:
        print(f"\n  --- pass: {label} ---")
        peaks, peak_props = find_peaks(p_elev, prominence=args.prominence)
        troughs, trough_props = find_peaks(-p_elev, prominence=args.prominence)
        print(f"  find_peaks(prominence={args.prominence}m): {len(peaks)} interior peaks, {len(troughs)} interior troughs")
        for i, p in enumerate(peaks):
            print(f"    peak   #{i}: idx={p}  dist={p_cum[p]:.0f} m  elev={p_elev[p]:.1f} m  prom={peak_props['prominences'][i]:.1f} m")
        for i, t in enumerate(troughs):
            print(f"    trough #{i}: idx={t}  dist={p_cum[t]:.0f} m  elev={p_elev[t]:.1f} m  prom={trough_props['prominences'][i]:.1f} m")

        extrema = find_extrema_with_boundary(p_elev, args.prominence)
        # Note which entries are synthetic boundary points (idx 0 or last)
        print(f"  extrema with synthetic boundaries: {len(extrema)} ({extrema[0]} ... {extrema[-1]})")
        print(f"  candidate trough→peak pairs:")
        if len(extrema) < 2:
            print(f"    NONE — profile too short")
        accepted: list[tuple[int, int, float, float, float]] = []
        for j in range(len(extrema) - 1):
            i_idx, i_kind = extrema[j]
            k_idx, k_kind = extrema[j + 1]
            if i_kind != "trough" or k_kind != "peak":
                print(f"    skip pair {i_kind}@{i_idx} → {k_kind}@{k_idx} (not trough→peak)")
                continue
            length = float(p_cum[k_idx] - p_cum[i_idx])
            gain = float(p_elev[k_idx] - p_elev[i_idx])
            grade = gain / length if length > 0 else 0
            reasons = []
            if grade <= 0:
                reasons.append(f"grade {grade*100:.2f}% <= 0")
            if length < args.min_length:
                reasons.append(f"length {length:.0f} < {args.min_length}")
            if grade < args.min_grade:
                reasons.append(f"grade {grade*100:.2f}% < {args.min_grade*100:.1f}%")
            if gain < args.min_gain:
                reasons.append(f"gain {gain:.1f} < {args.min_gain}")
            verdict = "ACCEPT" if not reasons else "REJECT (" + "; ".join(reasons) + ")"
            print(f"    trough@{i_idx} → peak@{k_idx}: length={length:.0f} m  grade={grade*100:.2f}%  gain={gain:.1f} m  → {verdict}")
            if not reasons:
                accepted.append((i_idx, k_idx, length, grade, gain))
        accepted_per_pass.append((label, accepted))

    if args.debug_plot:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(cum, elev_raw, color="#bbbbbb", linewidth=1, label="raw DEM")
        ax.plot(cum, elev, color="#1f77b4", linewidth=2, label=f"smoothed ({args.smooth_window} m)")
        all_peaks, _ = find_peaks(elev, prominence=args.prominence)
        all_troughs, _ = find_peaks(-elev, prominence=args.prominence)
        if len(all_peaks):
            ax.scatter(cum[all_peaks], elev[all_peaks], color="#d62728", zorder=5, label="peaks", s=40)
        if len(all_troughs):
            ax.scatter(cum[all_troughs], elev[all_troughs], color="#2ca02c", zorder=5, label="troughs", s=40)
        # Forward-pass accepted climbs: shade green; reverse-pass: shade orange.
        # For reverse, indices are in reversed array; map back to forward distance via cum[-1] - p_cum.
        for label, accepted in accepted_per_pass:
            color = "#00b050" if label == "forward" else "#ff7f0e"
            for ti, pi, length, grade, gain in accepted:
                if label == "forward":
                    x0, x1 = cum[ti], cum[pi]
                else:
                    # ti, pi are indices into reversed cum; convert to forward distance
                    n = len(cum) - 1
                    x0 = cum[n - pi]
                    x1 = cum[n - ti]
                ax.axvspan(x0, x1, color=color, alpha=0.2, label=f"{label} climb")
        # Dedupe legend
        handles, labels = ax.get_legend_handles_labels()
        seen = set()
        uniq = [(h, l) for h, l in zip(handles, labels) if not (l in seen or seen.add(l))]
        ax.legend([h for h, _ in uniq], [l for _, l in uniq], loc="best")
        ax.set_xlabel("distance along way (m)")
        ax.set_ylabel("elevation (m)")
        title = f"chain seed=way/{chain.primary_id} ({len(chain.way_ids)} way(s))"
        if chain.name:
            title += f" — {chain.name}"
        title += f" [{chain.highway}]"
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        suffix = f"chain_{chain.primary_id}"
        out_path = f"{args.debug_plot}/{suffix}.png" if args.debug_plot.endswith("/") else f"{args.debug_plot}_{suffix}.png"
        fig.tight_layout()
        fig.savefig(out_path, dpi=120)
        plt.close(fig)
        print(f"  plot saved: {out_path}")


def deduplicate_climbs(
    climbs: list[DetectedClimb], max_similarity: float
) -> tuple[list[DetectedClimb], int]:
    """Drop near-duplicates: pairs whose node-sets overlap by min(|A|, |B|) ≥ max_similarity.

    Score is a penalty (lower = better), so the lower-scored climb survives. Visits in
    ascending score order; each kept climb evicts its still-unprocessed neighbours.
    """
    if not climbs or max_similarity >= 1.0:
        return climbs, 0

    node_sets = [set(dc.nodes) for dc in climbs]
    node_index: dict[tuple[float, float], list[int]] = defaultdict(list)
    for i, ns in enumerate(node_sets):
        for n in ns:
            node_index[n].append(i)

    order = sorted(range(len(climbs)), key=lambda i: climbs[i].score)
    removed: set[int] = set()
    for i in order:
        if i in removed or not node_sets[i]:
            continue
        overlap: dict[int, int] = defaultdict(int)
        for n in node_sets[i]:
            for j in node_index[n]:
                if j == i or j in removed:
                    continue
                overlap[j] += 1
        size_i = len(node_sets[i])
        for j, count in overlap.items():
            denom = min(size_i, len(node_sets[j]))
            if denom and count / denom >= max_similarity:
                removed.add(j)

    kept = [c for idx, c in enumerate(climbs) if idx not in removed]
    return kept, len(removed)


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


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--pbf", required=True, help="Path to .osm.pbf input")
    ap.add_argument("--dem", required=True, help="Path to DEM GeoTIFF (EPSG:4326)")
    ap.add_argument("--db", required=True, help="Postgres DSN")
    ap.add_argument("--min-length", type=float, default=300.0, help="Min climb length for output (m)")
    ap.add_argument("--min-grade", type=float, default=0.01, help="Min average grade for output (decimal). Candidates with grade <= 0 are always rejected.")
    ap.add_argument("--min-gain", type=float, default=0.0, help="Min elevation gain (m); 0 disables")
    ap.add_argument("--sample-step", type=float, default=10.0, help="Resample spacing (m)")
    ap.add_argument("--smooth-window", type=float, default=100.0, help="Elevation smoothing window (m)")
    ap.add_argument("--prominence", type=float, default=10.0, help="Peak prominence threshold (m)")
    ap.add_argument("--max-combo", type=int, default=4, help="Max climbs to chain into a combination (>= 2)")
    ap.add_argument("--max-similarity", type=float, default=0.85,
                    help="Drop climbs whose node-set overlaps another's by min(|A|,|B|) >= this; "
                         "the climb with the lower score survives. Use 1.0 to disable.")
    ap.add_argument("--dry-run", action="store_true", help="Print stats, don't insert")
    ap.add_argument("-v", "--verbose", action="store_true", help="Print each detected climb")
    ap.add_argument(
        "--out-geojson",
        help="Write candidate ways + detected climbs to a GeoJSON file for visual inspection",
    )
    ap.add_argument(
        "--debug-way",
        type=int,
        action="append",
        metavar="OSM_WAY_ID",
        help="Find the chain containing this OSM way id and dump its full pipeline trace. Repeatable.",
    )
    ap.add_argument(
        "--debug-plot",
        metavar="PREFIX",
        help="Save elevation profile PNG when --debug-way is set. "
             "Pass a prefix like 'debug/' (saved to debug/way_<id>.png) or 'profile' (profile_<id>.png).",
    )
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    log.info("loading ways from %s", args.pbf)
    # In debug mode we still load the full network so chain stitching is correct.
    ways = load_ways(args.pbf)
    if not ways:
        log.error("no cycling network found in %s", args.pbf)
        return 1
    log.info("loaded %d ways", len(ways))

    log.info("computing node degrees")
    node_degree = compute_node_degree(ways)

    log.info("stitching chains")
    chains = build_chains(ways)
    log.info(
        "built %d chains (avg %.1f ways/chain, max %d)",
        len(chains),
        sum(len(c.way_ids) for c in chains) / max(1, len(chains)),
        max((len(c.way_ids) for c in chains), default=0),
    )

    log.info("opening DEM %s", args.dem)
    dem = rasterio.open(args.dem)
    if dem.crs and dem.crs.to_epsg() != 4326:
        log.warning("DEM CRS is %s, expected EPSG:4326 — sampling may be incorrect", dem.crs)

    if args.debug_way:
        only_ids = set(args.debug_way)
        ways_by_id = {w.id: w for w in ways}
        try:
            seen_chains: set[int] = set()
            for chain_idx, chain in enumerate(chains):
                if not (only_ids & set(chain.way_ids)):
                    continue
                if chain_idx in seen_chains:
                    continue
                seen_chains.add(chain_idx)
                debug_chain(rotate_loop_chain(chain, dem), ways_by_id, dem, args)
            all_way_ids = {w.id for w in ways}
            missing = only_ids - all_way_ids
            if missing:
                print(f"\nNOT FOUND in PBF: {sorted(missing)}")
        finally:
            dem.close()
        return 0

    detected: list[DetectedClimb] = []
    highway_counts: dict[str, int] = {}
    geojson_features: list[dict] = []

    try:
        for chain in tqdm(chains, unit="chain"):
            chain = rotate_loop_chain(chain, dem)
            if args.out_geojson:
                geojson_features.append(chain_feature(chain))
            resampled = resample_way(chain.coords, args.sample_step)
            if resampled is None:
                continue
            lats, lngs, cum = resampled
            if cum[-1] < args.min_length:
                continue
            elev = sample_elevation(dem, lats, lngs)
            if np.any(np.isnan(elev)):
                continue
            elev = smooth(elev, args.smooth_window, args.sample_step)

            chain_cum = cumulative_distances(chain.coords)
            chain_total = chain_cum[-1] if chain_cum else 0.0

            passes: list[tuple[bool, np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = [
                (False, lats, lngs, cum, elev),
            ]
            if chain.bidirectional:
                rl, rln, rc, re_ = reverse_profile(lats, lngs, cum, elev)
                passes.append((True, rl, rln, rc, re_))

            chain_name = chain_display_name(chain)

            for reversed_pass, p_lats, p_lngs, p_cum, p_elev in passes:
                for climb, ti, pi in detect_climbs(
                    p_lats, p_lngs, p_cum, p_elev,
                    args.min_length, args.min_grade, args.min_gain, args.prominence,
                ):
                    nodes, node_wids, node_surfs = chain_node_slice(
                        chain, chain_cum, chain_total,
                        p_cum, ti, pi, reversed_pass,
                    )
                    elevation_profile = [float(x) for x in p_elev[ti : pi + 1]]
                    dc = DetectedClimb(
                        climb=climb,
                        name=chain_name,
                        surfaces=unique_in_order(node_surfs),
                        highway=chain.highway,
                        osm_way_ids=unique_in_order(node_wids),
                        bidirectional=chain.bidirectional,
                        elevation_profile=elevation_profile,
                        nodes=nodes,
                        node_way_ids=node_wids,
                        node_surfaces=node_surfs,
                        score=compute_score(
                            nodes, elevation_profile, climb.length_m,
                            args.sample_step, node_degree,
                        ),
                    )
                    detected.append(dc)
                    highway_counts[chain.highway] = highway_counts.get(chain.highway, 0) + 1
                    if args.out_geojson:
                        geojson_features.append(climb_feature(climb, chain))
                    if args.verbose:
                        log.info(
                            "climb: %-40s  %5.0f m  %4.1f%%  +%4.0f m  start=%.5f,%.5f",
                            chain_name[:40],
                            climb.length_m,
                            climb.grade * 100,
                            climb.gain_m,
                            climb.coords[0][0],
                            climb.coords[0][1],
                        )

        log.info("detected %d per-chain climbs; building combinations", len(detected))
        combinations = build_combinations(
            detected, dem, args, node_degree,
            geojson_features=geojson_features if args.out_geojson else None,
        )
        log.info("built %d combination climbs", len(combinations))
        for dc in combinations:
            highway_counts[dc.highway] = highway_counts.get(dc.highway, 0) + 1
            if args.verbose:
                log.info(
                    "combo: %-40s  %5.0f m  %4.1f%%  +%4.0f m",
                    dc.name[:40],
                    dc.climb.length_m,
                    dc.climb.grade * 100,
                    dc.climb.gain_m,
                )
        detected.extend(combinations)
    finally:
        dem.close()

    before = len(detected)
    detected, dropped = deduplicate_climbs(detected, args.max_similarity)
    log.info(
        "deduplicated: %d → %d climbs (dropped %d at similarity >= %.2f)",
        before, len(detected), dropped, args.max_similarity,
    )

    rows = [to_climb_row(dc) for dc in detected]
    lengths = [dc.climb.length_m for dc in detected]
    grades = [dc.climb.grade for dc in detected]

    log.info("detected %d climbs total", len(rows))
    if highway_counts:
        breakdown = ", ".join(
            f"{hw}={n}" for hw, n in sorted(highway_counts.items(), key=lambda kv: -kv[1])
        )
        log.info("by highway: %s", breakdown)
    if lengths:
        ls = np.array(lengths)
        gs = np.array(grades) * 100
        log.info(
            "length (m): min=%.0f median=%.0f max=%.0f",
            ls.min(), np.median(ls), ls.max(),
        )
        log.info(
            "grade  (%%): min=%.1f median=%.1f max=%.1f",
            gs.min(), np.median(gs), gs.max(),
        )

    if args.out_geojson:
        write_geojson(args.out_geojson, geojson_features)
        log.info("wrote %d features to %s", len(geojson_features), args.out_geojson)

    if args.dry_run:
        log.info("--dry-run: not inserting")
        return 0

    log.info("inserting %d climbs", len(rows))
    insert_climbs(rows, args.db)
    log.info("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
