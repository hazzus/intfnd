"""Detect climbs on a chain's elevation profile.

Pipeline per chain:
    resample → DEM sample → smooth → find prominent extrema (with synthetic
    boundaries) → enumerate trough→peak pairs → filter by length/grade/gain →
    greedy non-overlap selection.

The forward pass walks the chain coords[0]→coords[-1]; bidirectional chains also
get a reverse pass so descents-as-climbs are picked up.
"""
from dataclasses import dataclass, field

import numpy as np
from scipy.signal import find_peaks

from .chains import Chain, chain_display_name
from .elevation import reverse_profile, sample_elevation, smooth
from .geo import cumulative_distances, resample_way
from .score import score_breakdown


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
    score_components: dict = field(default_factory=dict)


def unique_in_order(items):
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


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


def detect_in_chain(
    chain: Chain,
    dem,
    args,
    node_degree: dict[tuple[float, float], int],
) -> list[DetectedClimb]:
    """Run the per-chain climb detection pipeline. No side effects (no geojson, no logs).

    The chain should already be rotated for closed loops. Returns a list of fully-populated
    DetectedClimb objects (with score and score_components). Used by both the main detection
    loop and the --debug-way branch.
    """
    out: list[DetectedClimb] = []

    resampled = resample_way(chain.coords, args.sample_step)
    if resampled is None:
        return out
    lats, lngs, cum = resampled
    if cum[-1] < args.min_length:
        return out
    elev = sample_elevation(dem, lats, lngs)
    if np.any(np.isnan(elev)):
        return out
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
                chain, chain_cum, chain_total, p_cum, ti, pi, reversed_pass,
            )
            elevation_profile = [float(x) for x in p_elev[ti : pi + 1]]
            sb = score_breakdown(
                nodes, elevation_profile, climb.length_m,
                args.sample_step, node_degree,
            )
            out.append(DetectedClimb(
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
                score=sb["score"],
                score_components=sb,
            ))
    return out
