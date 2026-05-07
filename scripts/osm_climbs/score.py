"""Climb quality score in [0, 1]; lower is better.

Combines three saturated subscores: intersection density, average turn sharpness
at intersections, and point-to-point grade variability ("spike"). Each subscore
is normalised against a saturation threshold so the final score is bounded.
"""
import logging

import numpy as np

from .geo import bearing

log = logging.getLogger(__name__)

# ── score tuning constants ───────────────────────────────────────────────
# Saturation thresholds: subscore reaches 1.0 (worst) when its raw measurement
# hits the threshold. Lower threshold → faster saturation → harsher penalty.
SCORE_INTER_NORM = 5    # intersections per km
SCORE_TURN_NORM = 1.5     # sum((angle/ref)^2) / km^TURN_LENGTH_EXP
SCORE_SPIKE_NORM = 6.0    # max grade deviation / avg grade (unitless ratio)
SCORE_SIGNAL_NORM = 0.5   # signals per km that saturates score at 1.0 (1 light/2km = max penalty)

# Per-turn severity = (angle / TURN_REF_DEG)^2: super-linear so a single 90° turn
# (severity 1.0) outweighs four 30° turns (4 × 0.111 = 0.44). Severity is summed
# over the climb, then divided by length_km^TURN_LENGTH_EXP — sub-linear length
# normalization so a long climb with many gentle bends isn't over-penalised but
# a single sharp turn on a long climb isn't washed out either.
SCORE_TURN_REF_DEG = 90.0
SCORE_TURN_LENGTH_EXP = 0.5

# Weights mixing the three saturated subscores into the final score.
# Should sum to 1.0 if you want the final score to stay in [0, 1].
SCORE_WEIGHT_INTER = 0.30
SCORE_WEIGHT_TURN = 0.20
SCORE_WEIGHT_SPIKE = 0.10
SCORE_WEIGHT_SIGNALS = 0.40

# ── intersection weighting ───────────────────────────────────────────────
# Each OSM highway class gets a magnitude. At an intersection, the crossing
# way's magnitude is divided by the climb's own magnitude — so a primary
# crossing a residential climb counts heavily, while a residential branching
# off a primary climb barely registers. Only the *intersection density*
# component uses this weight; turn severity remains unweighted.
HIGHWAY_MAGNITUDE: dict[str, float] = {
    "motorway": 10, "motorway_link": 10,
    "trunk": 8, "trunk_link": 8,
    "primary": 6, 
    "primary_link": 4, "secondary": 4, 
    "secondary_link": 3, "tertiary": 3, 
    "tertiary_link": 2, "unclassified": 2,
    "residential": 2,
    "living_street": 1.5,
    "service": 1,
    "track": 1,
    "cycleway": 0.5,
    "path": 0.3, "footway": 0.3, "bridleway": 0.3,
    "pedestrian": 0.2, "steps": 0.2,
}
DEFAULT_MAGNITUDE = 1.0
MIN_CROSSING_WEIGHT = 0.1

# Module-level lookups. Configured once at startup by
# `configure_intersection_lookups` so score_breakdown doesn't need them
# threaded through every call site. When unset, weighting falls back to 1.0
# (preserves pre-feature behavior).
_node_ways_map: dict[tuple[float, float], set[int]] | None = None
_way_highways: dict[int, str] | None = None
_traffic_signals: set[tuple[float, float]] | None = None


def configure_intersection_lookups(
    node_ways_map: dict[tuple[float, float], set[int]],
    way_highways: dict[int, str],
    traffic_signals: set[tuple[float, float]] | None = None,
) -> None:
    """Register the global node→ways, way→highway, and traffic-signal maps."""
    global _node_ways_map, _way_highways, _traffic_signals
    _node_ways_map = node_ways_map
    _way_highways = way_highways
    _traffic_signals = traffic_signals or set()


def crossing_weight(climb_highway: str, crossing_highways: set[str]) -> float:
    """Severity multiplier for a crossing relative to the climb's own road class.

    `weight = max(crossing_mag / climb_mag, MIN_CROSSING_WEIGHT)`. When the crossing
    set is empty (or magnitude lookups not configured), returns 1.0 — the same as
    the pre-weighting baseline.
    """
    if not crossing_highways:
        return 1.0
    climb_mag = HIGHWAY_MAGNITUDE.get(climb_highway, DEFAULT_MAGNITUDE)
    if climb_mag <= 0:
        climb_mag = DEFAULT_MAGNITUDE
    crossing_mag = max(
        HIGHWAY_MAGNITUDE.get(hw, DEFAULT_MAGNITUDE) for hw in crossing_highways
    )
    return max(crossing_mag / climb_mag, MIN_CROSSING_WEIGHT)


def score_breakdown(
    nodes: list[tuple[float, float]],
    elevation_profile: list[float],
    length_m: float,
    sample_step: float,
    node_degree: dict[tuple[float, float], int],
    node_way_ids: list[int] | None = None,
) -> dict:
    """Compute the climb score and the per-component values that fed into it.

    `node_way_ids` is parallel to `nodes`; combined with the module-level
    lookups (set via `configure_intersection_lookups`) it lets each intersection
    be weighted relative to the climb's own road class. When either is missing,
    the weighting falls back to 1.0 per intersection.

    Returns a dict with: intersections, intersections_weighted, turn_sum_deg,
    length_km, inter_density, inter_score, avg_turn_deg, turn_score, grade_std,
    spike_score, score. Score is in [0, 1]; lower is better.
    """
    if length_m <= 0 or len(nodes) < 2:
        return {
            "intersections": 0, "intersections_weighted": 0.0,
            "turn_sum_deg": 0.0, "length_km": 0.0,
            "inter_density": 0.0, "inter_score": 0.0,
            "avg_turn_deg": 0.0, "turn_severity_total": 0.0,
            "turn_severity": 0.0, "turn_score": 0.0,
            "turn_degrees": [], "turn_nodes": [], "turn_weights": [],
            "avg_grade": 0.0, "max_spike_dev": 0.0,
            "spike_ratio": 0.0, "spike_score": 0.0,
            "signal_count": 0, "signals_per_km": 0.0, "signal_score": 0.0,
            "score": 1.0,
        }

    on_climb_ways = set(node_way_ids) if node_way_ids else set()
    have_lookups = (
        _node_ways_map is not None
        and _way_highways is not None
        and node_way_ids is not None
        and len(node_way_ids) == len(nodes)
    )

    intersections = 0
    intersections_weighted = 0.0
    turn_sum_deg = 0.0
    turn_severity_total = 0.0
    turn_degrees: list[float] = []
    turn_nodes: list[tuple[float, float]] = []
    turn_weights: list[float] = []
    for i in range(1, len(nodes) - 1):
        if node_degree.get(nodes[i], 0) < 3:
            continue
        intersections += 1
        d = abs(bearing(nodes[i], nodes[i + 1]) - bearing(nodes[i - 1], nodes[i])) % 360.0
        if d > 180.0:
            d = 360.0 - d
        turn_sum_deg += d
        turn_severity_total += (d / SCORE_TURN_REF_DEG) ** 2

        if have_lookups:
            climb_hw = _way_highways.get(node_way_ids[i], "")
            crossing_ways = _node_ways_map.get(nodes[i], set()) - on_climb_ways
            crossing_hws = {
                _way_highways[w] for w in crossing_ways if w in _way_highways
            }
            w = crossing_weight(climb_hw, crossing_hws)
        else:
            w = 1.0
        intersections_weighted += w

        turn_degrees.append(d)
        turn_nodes.append(nodes[i])
        turn_weights.append(w)

    signal_count = sum(1 for n in nodes if n in _traffic_signals) if _traffic_signals else 0

    length_km = max(length_m / 1000.0, 0.1)
    inter_density = intersections_weighted / length_km
    inter_score = min(1.0, inter_density / SCORE_INTER_NORM)
    avg_turn_deg = (turn_sum_deg / intersections) if intersections else 0.0
    turn_severity = turn_severity_total / (length_km ** SCORE_TURN_LENGTH_EXP)
    turn_score = min(1.0, turn_severity / SCORE_TURN_NORM)

    if len(elevation_profile) >= 3 and sample_step > 0:
        elev_arr = np.asarray(elevation_profile, dtype=float)
        step_grade = np.diff(elev_arr) / sample_step
        avg_grade = float((elev_arr[-1] - elev_arr[0]) / length_m)
        max_spike_dev = float(np.max(np.abs(step_grade - avg_grade)))
    else:
        avg_grade = 0.0
        max_spike_dev = 0.0
    # Relative spike size: how many "avg grades" the worst segment deviates by.
    # A 20% ramp inside a 3% climb → ratio ≈ 5.67 (very spiky).
    # A uniform 9% climb → ratio ≈ 0 (smooth).
    spike_ratio = (max_spike_dev / avg_grade) if avg_grade > 0 else 0.0
    spike_score = min(1.0, spike_ratio / SCORE_SPIKE_NORM)

    signals_per_km = signal_count / length_km
    signal_score = min(1.0, signals_per_km / SCORE_SIGNAL_NORM)

    score = float(
        SCORE_WEIGHT_INTER * inter_score
        + SCORE_WEIGHT_TURN * turn_score
        + SCORE_WEIGHT_SPIKE * spike_score
        + SCORE_WEIGHT_SIGNALS * signal_score
    )
    return {
        "intersections": intersections,
        "intersections_weighted": intersections_weighted,
        "turn_sum_deg": turn_sum_deg,
        "length_km": length_km, "inter_density": inter_density,
        "inter_score": inter_score, "avg_turn_deg": avg_turn_deg,
        "turn_severity_total": turn_severity_total,
        "turn_severity": turn_severity, "turn_score": turn_score,
        "turn_degrees": turn_degrees, "turn_nodes": turn_nodes,
        "turn_weights": turn_weights,
        "avg_grade": avg_grade,
        "max_spike_dev": max_spike_dev, "spike_ratio": spike_ratio,
        "spike_score": spike_score,
        "signal_count": signal_count, "signals_per_km": signals_per_km,
        "signal_score": signal_score,
        "score": score,
    }


def compute_score(
    nodes: list[tuple[float, float]],
    elevation_profile: list[float],
    length_m: float,
    sample_step: float,
    node_degree: dict[tuple[float, float], int],
    node_way_ids: list[int] | None = None,
) -> float:
    """Climb quality score in [0, 1]; lower is better."""
    return score_breakdown(
        nodes, elevation_profile, length_m, sample_step, node_degree, node_way_ids,
    )["score"]


def log_score_stats(detected) -> None:
    """Log mean / median / max of each score component, plus saturation rates.

    Use this to tune the normalisation thresholds in score_breakdown — if a saturated
    subscore hits 1.0 for a large fraction of climbs, the threshold is too tight and
    the component effectively becomes a constant penalty.
    """
    if not detected:
        return

    rows = [dc.score_components for dc in detected if dc.score_components]
    if not rows:
        return

    def stats(values: list[float]) -> tuple[float, float, float, float]:
        a = np.asarray(values, dtype=float)
        return (
            float(a.mean()),
            float(np.median(a)),
            float(np.percentile(a, 90)),
            float(a.max()),
        )

    raw_keys = [
        ("intersections / km", "inter_density",  "%6.2f", SCORE_INTER_NORM),
        ("turn severity / √km", "turn_severity", "%6.2f", SCORE_TURN_NORM),
        ("spike ratio       ", "spike_ratio",    "%6.2f", SCORE_SPIKE_NORM),
        ("signals / km      ", "signals_per_km", "%6.2f", SCORE_SIGNAL_NORM),
    ]
    sat_keys = [
        ("inter_score ", "inter_score",  SCORE_WEIGHT_INTER),
        ("turn_score  ", "turn_score",   SCORE_WEIGHT_TURN),
        ("spike_score ", "spike_score",  SCORE_WEIGHT_SPIKE),
        ("signal_score", "signal_score", SCORE_WEIGHT_SIGNALS),
    ]

    log.info("score components — unnormalised (p90 is a good saturation-point candidate):")
    for label, key, fmt, norm in raw_keys:
        vals = [r.get(key, 0.0) for r in rows]
        m, med, p90, mx = stats(vals)
        log.info(
            "  %s mean=%s median=%s p90=%s max=%s (norm=%g)",
            label, fmt % m, fmt % med, fmt % p90, fmt % mx, norm,
        )

    log.info("score components — saturated [0, 1] (saturation = fraction at 1.0):")
    for label, key, weight in sat_keys:
        vals = [r.get(key, 0.0) for r in rows]
        m, med, p90, mx = stats(vals)
        sat = float(np.mean([v >= 0.999 for v in vals]))
        log.info(
            "  %s mean=%.3f median=%.3f p90=%.3f max=%.3f saturation=%.1f%% weight=%g",
            label, m, med, p90, mx, sat * 100.0, weight,
        )

    scores = [dc.score for dc in detected]
    m, med, p90, mx = stats(scores)
    log.info(
        "final score (raw [0,1], lower=better): mean=%.3f median=%.3f p90=%.3f max=%.3f",
        m, med, p90, mx,
    )
    displayed = np.array([100.0 * (1.0 - s) for s in scores])
    log.info(
        "final score (displayed 0-100, higher=better): mean=%.1f median=%.1f p10=%.1f min=%.1f max=%.1f",
        float(displayed.mean()), float(np.median(displayed)),
        float(np.percentile(displayed, 10)),
        float(displayed.min()), float(displayed.max()),
    )


def print_score_breakdown_lines(
    sb: dict,
    indent: str = "      ",
    *,
    other_highways_at=None,
) -> None:
    """Print the four-line score-component breakdown shared by debug_chain and debug_combinations.

    `other_highways_at`, if given, is a callable `node -> list[str]` returning the highway
    tags of ways meeting the climb at that intersection but not part of the climb itself.
    Passed through so debug callers can annotate the per-turn list.
    """
    inter_w = SCORE_WEIGHT_INTER * sb["inter_score"]
    turn_w = SCORE_WEIGHT_TURN * sb["turn_score"]
    spike_w = SCORE_WEIGHT_SPIKE * sb["spike_score"]
    print(f"{indent}score = {sb['score']:.3f}  (lower is better)")
    inter_w_count = sb.get("intersections_weighted", float(sb["intersections"]))
    print(
        f"{indent}intersections : {sb['intersections']:>3} (weighted {inter_w_count:.2f}) "
        f"on {sb['length_km']:.2f} km "
        f"({sb['inter_density']:.2f}/km, capped at {SCORE_INTER_NORM:g}/km) → "
        f"inter_score={sb['inter_score']:.3f} × {SCORE_WEIGHT_INTER:g} = {inter_w:.3f}"
    )
    print(
        f"{indent}turns         : avg {sb['avg_turn_deg']:5.1f}°  "
        f"severity_total={sb['turn_severity_total']:.3f}  "
        f"/ km^{SCORE_TURN_LENGTH_EXP:g} = {sb['turn_severity']:.3f} "
        f"(capped at {SCORE_TURN_NORM:g}) → "
        f"turn_score={sb['turn_score']:.3f} × {SCORE_WEIGHT_TURN:g} = {turn_w:.3f}"
    )
    turn_degrees = sb.get("turn_degrees") or []
    turn_nodes = sb.get("turn_nodes") or []
    turn_weights = sb.get("turn_weights") or []
    if turn_degrees:
        print(f"{indent}turn list     : severity = (angle/{SCORE_TURN_REF_DEG:g}°)^2, summed → turn_score")
        for i, d in enumerate(turn_degrees):
            tag = ""
            loc = ""
            wstr = ""
            if i < len(turn_nodes):
                lng, lat = turn_nodes[i]
                loc = f" @ {lat:.5f},{lng:.5f}"
                if other_highways_at is not None:
                    others = other_highways_at(turn_nodes[i])
                    if others:
                        tag = f"  [{','.join(others)}]"
            if i < len(turn_weights):
                wstr = f"  inter×{turn_weights[i]:.2f}"
            sev = (d / SCORE_TURN_REF_DEG) ** 2
            print(f"{indent}  severity={sev:.3f}  ({d:5.1f}°){loc}{tag}{wstr}")
    print(
        f"{indent}spike         : worst |step − avg| = {sb['max_spike_dev']*100:5.2f}% "
        f"vs avg {sb['avg_grade']*100:.2f}% → ratio {sb['spike_ratio']:.2f}× "
        f"(capped at {SCORE_SPIKE_NORM:g}×) → "
        f"spike_score={sb['spike_score']:.3f} × {SCORE_WEIGHT_SPIKE:g} = {spike_w:.3f}"
    )
    signal_w = SCORE_WEIGHT_SIGNALS * sb.get("signal_score", 0.0)
    print(
        f"{indent}signals       : {sb.get('signal_count', 0):>3} "
        f"on {sb['length_km']:.2f} km "
        f"({sb.get('signals_per_km', 0.0):.2f}/km, capped at {SCORE_SIGNAL_NORM:g}/km) → "
        f"signal_score={sb.get('signal_score', 0.0):.3f} × {SCORE_WEIGHT_SIGNALS:g} = {signal_w:.3f}"
    )
