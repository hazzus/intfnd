"""Trim a short prefix or suffix that joins the climb at a sharp turn.

A climb often starts on a feeder road that branches into the "real" climb at a
junction (and symmetrically may overshoot past a junction at the end). When the
first ≤ --max-strip meters end at an intersection where the path turns by
≥ --strip-degree, that prefix is removed and the climb is restarted at the
intersection. The same check is run from the other end and any qualifying
suffix is dropped.
"""
import logging

from . import score as score_mod
from .detect import Climb, DetectedClimb, unique_in_order
from .geo import bearing, cumulative_distances
from .score import print_score_breakdown_lines, score_breakdown

log = logging.getLogger(__name__)


def _turn_angle(
    a: tuple[float, float],
    b: tuple[float, float],
    c: tuple[float, float],
) -> float:
    d = abs(bearing(b, c) - bearing(a, b)) % 360.0
    if d > 180.0:
        d = 360.0 - d
    return d


def _strip_one(
    dc: DetectedClimb,
    max_strip: float,
    strip_degree: float,
    sample_step: float,
    node_degree: dict[tuple[float, float], int],
    debug: bool = False,
) -> tuple[DetectedClimb | None, list[dict], list[dict]]:
    """Return (new_climb_or_None, front_trace, back_trace).

    Each trace is a list of per-node decision records walked within max_strip
    from the respective end; only populated when debug is True. Used by the
    caller to emit `--debug-strip` output alongside the strip outcome.
    """
    nodes = dc.nodes
    if len(nodes) < 3:
        return None, [], []

    cum = cumulative_distances(nodes)
    total = cum[-1]

    # Forward walk: find the first qualifying intersection within max_strip.
    front_trace: list[dict] = []
    strip_start: int = 0
    for i in range(1, len(nodes) - 1):
        if cum[i] > max_strip:
            break
        deg = node_degree.get(nodes[i], 0)
        is_inter = deg >= 3
        angle = (
            _turn_angle(nodes[i - 1], nodes[i], nodes[i + 1])
            if is_inter else 0.0
        )
        if debug:
            front_trace.append({
                "node_idx": i,
                "node": nodes[i],
                "dist": cum[i],
                "degree": deg,
                "is_intersection": is_inter,
                "turn_deg": angle,
                "qualifies": is_inter and angle >= strip_degree,
            })
        if is_inter and angle >= strip_degree:
            strip_start = i
            break

    # Backward walk: find the last qualifying intersection within max_strip
    # of the end. Stop short of strip_start so the two strips can't collide.
    back_trace: list[dict] = []
    strip_end: int = len(nodes) - 1
    for i in range(len(nodes) - 2, strip_start, -1):
        dist_from_end = total - cum[i]
        if dist_from_end > max_strip:
            break
        deg = node_degree.get(nodes[i], 0)
        is_inter = deg >= 3
        angle = (
            _turn_angle(nodes[i - 1], nodes[i], nodes[i + 1])
            if is_inter else 0.0
        )
        if debug:
            back_trace.append({
                "node_idx": i,
                "node": nodes[i],
                "dist": cum[i],
                "dist_from_end": dist_from_end,
                "degree": deg,
                "is_intersection": is_inter,
                "turn_deg": angle,
                "qualifies": is_inter and angle >= strip_degree,
            })
        if is_inter and angle >= strip_degree:
            strip_end = i
            break

    if strip_start == 0 and strip_end == len(nodes) - 1:
        return None, front_trace, back_trace

    n_resampled = len(dc.climb.coords)
    if n_resampled < 3:
        return None, front_trace, back_trace
    # The resampled coords run at sample_step spacing along the same polyline as
    # the original chain nodes, so distances along `cum` map to a resampled
    # index by simple division. Sub-step misalignment between climb.coords[0]
    # and nodes[0] is bounded by one sample_step.
    drop_front = (
        max(1, int(round(cum[strip_start] / sample_step))) if strip_start > 0 else 0
    )
    drop_back = (
        max(1, int(round((total - cum[strip_end]) / sample_step)))
        if strip_end < len(nodes) - 1 else 0
    )
    end_idx = n_resampled - drop_back
    if end_idx - drop_front < 3:
        return None, front_trace, back_trace

    new_coords = dc.climb.coords[drop_front:end_idx]
    new_elev = dc.elevation_profile[drop_front:end_idx]
    new_length = (len(new_coords) - 1) * sample_step
    if new_length <= 0:
        return None, front_trace, back_trace
    new_gain = float(new_elev[-1] - new_elev[0])
    if new_gain <= 0:
        return None, front_trace, back_trace
    new_grade = new_gain / new_length

    new_nodes = list(nodes[strip_start:strip_end + 1])
    new_node_wids = list(dc.node_way_ids[strip_start:strip_end + 1])
    new_node_surfs = list(dc.node_surfaces[strip_start:strip_end + 1])

    sb = score_breakdown(
        new_nodes, new_elev, new_length,
        sample_step, node_degree, new_node_wids,
    )
    new_dc = DetectedClimb(
        climb=Climb(
            coords=new_coords,
            length_m=new_length,
            grade=new_grade,
            gain_m=new_gain,
        ),
        name=dc.name,
        surfaces=unique_in_order(new_node_surfs),
        highway=dc.highway,
        osm_way_ids=unique_in_order(new_node_wids),
        bidirectional=dc.bidirectional,
        elevation_profile=new_elev,
        nodes=new_nodes,
        node_way_ids=new_node_wids,
        node_surfaces=new_node_surfs,
        is_combination=dc.is_combination,
        score=sb["score"],
        score_components=sb,
    )
    return new_dc, front_trace, back_trace


def _print_trace_section(label: str, trace: list[dict], dist_key: str) -> None:
    if not trace:
        print(f"  {label}: (no nodes walked within --max-strip)")
        return
    print(f"  {label}: walked {len(trace)} node(s):")
    for t in trace:
        lng, lat = t["node"]
        tag = []
        if t["is_intersection"]:
            tag.append(f"intersection deg={t['degree']}")
            tag.append(f"turn={t['turn_deg']:.1f}°")
            tag.append("QUALIFIES" if t["qualifies"] else "below --strip-degree")
        else:
            tag.append(f"deg={t['degree']} (not an intersection)")
        print(
            f"    node[{t['node_idx']:>3}] @ {lat:.5f},{lng:.5f}  "
            f"{dist_key}={t[dist_key]:6.1f} m  {' / '.join(tag)}"
        )


def _print_trace(
    dc: DetectedClimb,
    front_trace: list[dict],
    back_trace: list[dict],
    new_dc: DetectedClimb | None,
    outcome: str,
    sides: str = "",
    reason: str = "",
) -> None:
    start_lat, start_lng = dc.climb.coords[0]
    print(
        f"\n[strip] {dc.name!r}  ({dc.climb.length_m:.0f} m, {dc.climb.grade*100:.2f}%, "
        f"+{dc.climb.gain_m:.1f} m, start={start_lat:.5f},{start_lng:.5f})"
    )
    _print_trace_section("front walk", front_trace, "dist")
    _print_trace_section("back walk", back_trace, "dist_from_end")
    if outcome == "stripped":
        assert new_dc is not None
        print(
            f"  → STRIPPED ({sides}): new length {new_dc.climb.length_m:.0f} m, "
            f"grade {new_dc.climb.grade*100:.2f}%, gain +{new_dc.climb.gain_m:.1f} m, "
            f"start={new_dc.climb.coords[0][0]:.5f},{new_dc.climb.coords[0][1]:.5f}, "
            f"score {dc.score:.3f} → {new_dc.score:.3f}"
        )
        if new_dc.score_components:
            print_score_breakdown_lines(
                new_dc.score_components,
                indent="    ",
                other_highways_at=_make_other_highways_at(new_dc),
            )
    elif outcome == "dropped":
        assert new_dc is not None
        print(
            f"  → DROPPED ({sides}; {reason}): remainder would be "
            f"length {new_dc.climb.length_m:.0f} m, grade {new_dc.climb.grade*100:.2f}%, "
            f"gain +{new_dc.climb.gain_m:.1f} m"
        )
        if new_dc.score_components:
            print_score_breakdown_lines(
                new_dc.score_components,
                indent="    ",
                other_highways_at=_make_other_highways_at(new_dc),
            )
    elif outcome == "noop":
        print("  → no qualifying intersection within --max-strip on either end; kept unchanged")


def _make_other_highways_at(dc: DetectedClimb):
    """Closure: node → sorted highway tags of crossing ways at that node.

    Uses the module-level lookups configured on `score` so the strip trace
    matches the debug breakdown format. Falls back to no annotation when
    lookups aren't configured.
    """
    node_ways = score_mod._node_ways_map
    way_hw = score_mod._way_highways
    if node_ways is None or way_hw is None:
        return None
    on_climb = set(dc.osm_way_ids)

    def f(node: tuple[float, float]) -> list[str]:
        ws = node_ways.get(node, set()) - on_climb
        return sorted({way_hw[w] for w in ws if w in way_hw})

    return f


def _sides_label(front: bool, back: bool) -> str:
    if front and back:
        return "front+back"
    if front:
        return "front"
    if back:
        return "back"
    return ""


def strip_climbs(
    detected: list[DetectedClimb],
    max_strip: float,
    strip_degree: float,
    sample_step: float,
    min_length: float,
    min_grade: float,
    min_gain: float,
    node_degree: dict[tuple[float, float], int],
    debug: bool = False,
) -> tuple[list[DetectedClimb], int, int]:
    """Strip the prefix and/or suffix of each climb when it joins via a sharp intersection.

    Returns (kept, stripped, dropped). A climb is dropped when stripping pushes
    it below the original min-length / min-grade / min-gain thresholds — the
    remainder isn't a climb on its own.

    When debug is True, prints a per-climb trace for every climb where the
    walks surfaced at least one intersection within --max-strip from either end
    (covering both fired and considered-but-rejected cases).
    """
    out: list[DetectedClimb] = []
    stripped = 0
    dropped = 0
    for dc in detected:
        new_dc, front_trace, back_trace = _strip_one(
            dc, max_strip, strip_degree, sample_step, node_degree, debug=debug,
        )
        front_fired = any(t["qualifies"] for t in front_trace)
        back_fired = any(t["qualifies"] for t in back_trace)
        sides = _sides_label(front_fired, back_fired)
        had_intersection = (
            any(t["is_intersection"] for t in front_trace)
            or any(t["is_intersection"] for t in back_trace)
        )
        if new_dc is None:
            out.append(dc)
            if debug and had_intersection:
                _print_trace(dc, front_trace, back_trace, None, "noop")
            continue
        sub_threshold_reasons: list[str] = []
        if new_dc.climb.length_m < min_length:
            sub_threshold_reasons.append(f"length < {min_length:g}")
        if new_dc.climb.grade < min_grade:
            sub_threshold_reasons.append(f"grade < {min_grade*100:g}%")
        if new_dc.climb.gain_m < min_gain:
            sub_threshold_reasons.append(f"gain < {min_gain:g}")
        if sub_threshold_reasons:
            dropped += 1
            if debug:
                _print_trace(
                    dc, front_trace, back_trace, new_dc, "dropped",
                    sides=sides, reason="; ".join(sub_threshold_reasons),
                )
            continue
        out.append(new_dc)
        stripped += 1
        if debug:
            _print_trace(dc, front_trace, back_trace, new_dc, "stripped", sides=sides)
    return out, stripped, dropped
