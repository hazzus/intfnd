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
SCORE_INTER_NORM = 5.0    # intersections per km
SCORE_TURN_NORM = 1.5     # sum((angle/ref)^2) / km^TURN_LENGTH_EXP
SCORE_SPIKE_NORM = 6.0    # max grade deviation / avg grade (unitless ratio)

# Per-turn severity = (angle / TURN_REF_DEG)^2: super-linear so a single 90° turn
# (severity 1.0) outweighs four 30° turns (4 × 0.111 = 0.44). Severity is summed
# over the climb, then divided by length_km^TURN_LENGTH_EXP — sub-linear length
# normalization so a long climb with many gentle bends isn't over-penalised but
# a single sharp turn on a long climb isn't washed out either.
SCORE_TURN_REF_DEG = 90.0
SCORE_TURN_LENGTH_EXP = 0.5

# Weights mixing the three saturated subscores into the final score.
# Should sum to 1.0 if you want the final score to stay in [0, 1].
SCORE_WEIGHT_INTER = 0.5
SCORE_WEIGHT_TURN = 0.4
SCORE_WEIGHT_SPIKE = 0.1


def score_breakdown(
    nodes: list[tuple[float, float]],
    elevation_profile: list[float],
    length_m: float,
    sample_step: float,
    node_degree: dict[tuple[float, float], int],
) -> dict:
    """Compute the climb score and the per-component values that fed into it.

    Returns a dict with: intersections, turn_sum_deg, length_km, inter_density,
    inter_score, avg_turn_deg, turn_score, grade_std, spike_score, score.
    Score is in [0, 1]; lower is better.
    """
    if length_m <= 0 or len(nodes) < 2:
        return {
            "intersections": 0, "turn_sum_deg": 0.0, "length_km": 0.0,
            "inter_density": 0.0, "inter_score": 0.0,
            "avg_turn_deg": 0.0, "turn_severity_total": 0.0,
            "turn_severity": 0.0, "turn_score": 0.0,
            "avg_grade": 0.0, "max_spike_dev": 0.0,
            "spike_ratio": 0.0, "spike_score": 0.0,
            "score": 1.0,
        }

    intersections = 0
    turn_sum_deg = 0.0
    turn_severity_total = 0.0
    for i in range(1, len(nodes) - 1):
        if node_degree.get(nodes[i], 0) < 3:
            continue
        intersections += 1
        d = abs(bearing(nodes[i], nodes[i + 1]) - bearing(nodes[i - 1], nodes[i])) % 360.0
        if d > 180.0:
            d = 360.0 - d
        turn_sum_deg += d
        turn_severity_total += (d / SCORE_TURN_REF_DEG) ** 2

    length_km = max(length_m / 1000.0, 0.1)
    inter_density = intersections / length_km
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

    score = float(
        SCORE_WEIGHT_INTER * inter_score
        + SCORE_WEIGHT_TURN * turn_score
        + SCORE_WEIGHT_SPIKE * spike_score
    )
    return {
        "intersections": intersections, "turn_sum_deg": turn_sum_deg,
        "length_km": length_km, "inter_density": inter_density,
        "inter_score": inter_score, "avg_turn_deg": avg_turn_deg,
        "turn_severity_total": turn_severity_total,
        "turn_severity": turn_severity, "turn_score": turn_score,
        "avg_grade": avg_grade,
        "max_spike_dev": max_spike_dev, "spike_ratio": spike_ratio,
        "spike_score": spike_score, "score": score,
    }


def compute_score(
    nodes: list[tuple[float, float]],
    elevation_profile: list[float],
    length_m: float,
    sample_step: float,
    node_degree: dict[tuple[float, float], int],
) -> float:
    """Climb quality score in [0, 1]; lower is better."""
    return score_breakdown(nodes, elevation_profile, length_m, sample_step, node_degree)["score"]


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
    ]
    sat_keys = [
        ("inter_score", "inter_score", SCORE_WEIGHT_INTER),
        ("turn_score ", "turn_score",  SCORE_WEIGHT_TURN),
        ("spike_score", "spike_score", SCORE_WEIGHT_SPIKE),
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


def print_score_breakdown_lines(sb: dict, indent: str = "      ") -> None:
    """Print the four-line score-component breakdown shared by debug_chain and debug_combinations."""
    inter_w = SCORE_WEIGHT_INTER * sb["inter_score"]
    turn_w = SCORE_WEIGHT_TURN * sb["turn_score"]
    spike_w = SCORE_WEIGHT_SPIKE * sb["spike_score"]
    print(f"{indent}score = {sb['score']:.3f}  (lower is better)")
    print(
        f"{indent}intersections : {sb['intersections']:>3} on {sb['length_km']:.2f} km "
        f"({sb['inter_density']:.2f}/km, capped at {SCORE_INTER_NORM:g}/km) → "
        f"inter_score={sb['inter_score']:.3f} × {SCORE_WEIGHT_INTER:g} = {inter_w:.3f}"
    )
    print(
        f"{indent}avg turn      : {sb['avg_turn_deg']:5.1f}° "
        f"(capped at {SCORE_TURN_NORM:g}°) → "
        f"turn_score={sb['turn_score']:.3f} × {SCORE_WEIGHT_TURN:g} = {turn_w:.3f}"
    )
    print(
        f"{indent}spike         : worst |step − avg| = {sb['max_spike_dev']*100:5.2f}% "
        f"vs avg {sb['avg_grade']*100:.2f}% → ratio {sb['spike_ratio']:.2f}× "
        f"(capped at {SCORE_SPIKE_NORM:g}×) → "
        f"spike_score={sb['spike_score']:.3f} × {SCORE_WEIGHT_SPIKE:g} = {spike_w:.3f}"
    )
