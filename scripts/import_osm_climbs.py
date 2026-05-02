#!/usr/bin/env python3
"""Extract climbs from OSM PBF + DEM and insert into the segments table.

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


@dataclass
class Climb:
    coords: list[tuple[float, float]]
    length_m: float
    grade: float
    gain_m: float


@dataclass
class Way:
    id: int
    coords: list[tuple[float, float]]  # (lng, lat)
    name: str | None
    ref: str | None
    highway: str
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
        self.ways.append(
            Way(
                id=w.id,
                coords=coords,
                name=tags.get("name"),
                ref=tags.get("ref"),
                highway=hw or "<none>",
                tags={t.k: t.v for t in tags},
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
    name: str | None
    ref: str | None
    highway: str
    bidirectional: bool
    tags: dict = field(default_factory=dict)

    @property
    def primary_id(self) -> int:
        return self.way_ids[0]


def build_chains(ways: list[Way]) -> list[Chain]:
    """Stitch ways into chains through OSM nodes that are degree-2 (no junction).

    Two ways are stitched only when they share an endpoint node and that node
    has exactly two way-endpoints attached to it. Ways with start == end
    (closed loops) are not stitched.
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

    # partner[(way, port)] = (other_way, other_port) only when shared node has degree 2
    partner: dict[tuple[int, str], tuple[int, str]] = {}
    for ports in endpoints.values():
        if len(ports) != 2:
            continue
        (i1, p1), (i2, p2) = ports
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

        # Concatenate coords, deduping the shared boundary node between adjacent ways.
        combined: list[tuple[float, float]] = []
        for k, (way_idx, rev) in enumerate(ordered):
            seg = ways[way_idx].coords if not rev else list(reversed(ways[way_idx].coords))
            if k == 0:
                combined.extend(seg)
            else:
                combined.extend(seg[1:])

        members = [ways[i] for i, _ in ordered]
        highway = Counter(m.highway for m in members).most_common(1)[0][0]
        names = [m.name for m in members if m.name]
        seed_way = ways[seed]
        chains.append(
            Chain(
                way_ids=[ways[i].id for i, _ in ordered],
                coords=combined,
                name=seed_way.name or (names[0] if names else None),
                ref=seed_way.ref,
                highway=highway,
                bidirectional=all(is_bidirectional(m.tags) for m in members),
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
) -> list[Climb]:
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

    climbs = []
    for ti, pi, length, grade, gain in selected:
        coords = list(zip(lats[ti : pi + 1].tolist(), lngs[ti : pi + 1].tolist()))
        climbs.append(Climb(coords=coords, length_m=length, grade=grade, gain_m=gain))
    return climbs


def is_bidirectional(tags: dict) -> bool:
    """Return True if a cyclist can ride this way in both stored directions."""
    if tags.get("oneway:bicycle") == "no":
        return True
    oneway = tags.get("oneway", "no")
    return oneway in ("no", "false", "0", None)


def reverse_profile(lats, lngs, cum, elev):
    return (
        lats[::-1],
        lngs[::-1],
        cum[-1] - cum[::-1],
        elev[::-1],
    )


def to_segment_row(climb: Climb, way_name, way_ref) -> dict:
    if way_name:
        name = str(way_name)
    elif way_ref:
        name = f"Climb on {way_ref}"
    else:
        name = "Unnamed climb"
    return {
        "name": name,
        "distance": float(climb.length_m),
        "average_grade": float(climb.grade),
        "start_lat": float(climb.coords[0][0]),
        "start_lng": float(climb.coords[0][1]),
        "polyline": polyline_lib.encode(climb.coords),
        "star_count": 0,
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


def write_geojson(path: str, features: list[dict]) -> None:
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w") as f:
        json.dump(fc, f)


def debug_chain(chain: Chain, dem, args) -> None:
    print(f"\n=== chain (seed way {chain.primary_id}, {len(chain.way_ids)} way(s)) ===")
    print(f"  way_ids: {chain.way_ids}")
    print(f"  highway: {chain.highway}   name: {chain.name!r}   ref: {chain.ref!r}")
    print(f"  bidirectional: {chain.bidirectional}")
    print(f"  raw coords: {len(chain.coords)} nodes")

    resampled = resample_way(chain.coords, args.sample_step)
    if resampled is None:
        print("  RESAMPLE FAILED (way too short or zero-length segments)")
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


def insert_segments(rows: list[dict], dsn: str) -> None:
    if not rows:
        return
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO segments
                    (name, distance, average_grade, start_lat, start_lng, polyline, star_count)
                VALUES
                    (%(name)s, %(distance)s, %(average_grade)s,
                     %(start_lat)s, %(start_lng)s, %(polyline)s, %(star_count)s)
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
        try:
            seen_chains: set[int] = set()
            for chain_idx, chain in enumerate(chains):
                if not (only_ids & set(chain.way_ids)):
                    continue
                if chain_idx in seen_chains:
                    continue
                seen_chains.add(chain_idx)
                debug_chain(chain, dem, args)
            all_way_ids = {w.id for w in ways}
            missing = only_ids - all_way_ids
            if missing:
                print(f"\nNOT FOUND in PBF: {sorted(missing)}")
        finally:
            dem.close()
        return 0

    rows: list[dict] = []
    lengths: list[float] = []
    grades: list[float] = []
    highway_counts: dict[str, int] = {}
    geojson_features: list[dict] = []

    try:
        for chain in tqdm(chains, unit="chain"):
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

            passes = [(lats, lngs, cum, elev)]
            if chain.bidirectional:
                passes.append(reverse_profile(lats, lngs, cum, elev))

            for p_lats, p_lngs, p_cum, p_elev in passes:
                for climb in detect_climbs(
                    p_lats, p_lngs, p_cum, p_elev,
                    args.min_length, args.min_grade, args.min_gain, args.prominence,
                ):
                    row = to_segment_row(climb, chain.name, chain.ref)
                    rows.append(row)
                    lengths.append(climb.length_m)
                    grades.append(climb.grade)
                    highway_counts[chain.highway] = highway_counts.get(chain.highway, 0) + 1
                    if args.out_geojson:
                        geojson_features.append(climb_feature(climb, chain))
                    if args.verbose:
                        log.info(
                            "climb: %-40s  %5.0f m  %4.1f%%  +%4.0f m  start=%.5f,%.5f  polyline=%s",
                            row["name"][:40],
                            climb.length_m,
                            climb.grade * 100,
                            climb.gain_m,
                            climb.coords[0][0],
                            climb.coords[0][1],
                            row["polyline"],
                        )
    finally:
        dem.close()

    log.info("detected %d climbs", len(rows))
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

    log.info("inserting %d segments", len(rows))
    insert_segments(rows, args.db)
    log.info("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
